/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl2.workflow.remote;

import java.util.concurrent.Callable;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.icgc.dcc.etl2.core.job.Job;
import org.springframework.jmx.access.MBeanProxyFactoryBean;
import org.springframework.jmx.support.MBeanServerConnectionFactoryBean;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoteJobExecutor {

  public void submit(Class<? extends Job> jobClass) throws Exception {
    val process = new RemoteJobProcess(jobClass);

    log.info("Starting process {}...", process);
    process.start();
    log.info("Started process {}", process);

    try {
      log.info("Connecting to server on port {}", process.getPort());
      val serverConnection = connect(process.getPort());
      log.info("Connected to server {}", serverConnection);

      log.info("Resolving job service...");
      val jobService = resolveJobService(serverConnection);
      log.info("Resolved job service");

      execute(jobService);
    } catch (Exception e) {
      process.close();
      throw e;
    }
  }

  private void execute(final org.icgc.dcc.etl2.workflow.remote.RemoteJobService jobService) {
    val properties = ImmutableMap.<String, Object> of("x", 1);

    log.info("Executing job service {}...", jobService.getName());
    jobService.execute(properties);
  }

  @SneakyThrows
  private MBeanServerConnection connect(int port) {
    val serviceUrl = "service:jmx:rmi:///jndi/rmi://localhost:" + port + "/jmxrmi";
    val serverFactory = new MBeanServerConnectionFactoryBean();
    serverFactory.setServiceUrl(serviceUrl);

    return await(serviceUrl, () -> {
      // Are we there yet?
      serverFactory.afterPropertiesSet();

      // Yep!
      return serverFactory.getObject();
    });
  }

  @SneakyThrows
  private RemoteJobService resolveJobService(MBeanServerConnection server) {
    val objectName = new ObjectName(JMXRemoteJobService.JMX_OBJECT_NAME);
    val proxy = new MBeanProxyFactoryBean();

    return await(objectName, () -> {
      // Are we there yet?
      server.getMBeanInfo(objectName);

      // Yep!
      proxy.setServer(server);
      proxy.setObjectName(objectName);
      proxy.setProxyInterface(RemoteJobService.class);
      proxy.afterPropertiesSet();

      return (RemoteJobService) proxy.getObject();
    });
  }

  @SneakyThrows
  private <T> T await(Object resource, Callable<T> producer) {
    val delay = 500; // ms
    val watch = Stopwatch.createStarted();
    for (int i = 0; i < 4; i++) {
      try {
        return producer.call();
      } catch (Exception e) {
        // Not ready yet :(
        log.info("Waiting for {}: {}", resource, watch);
        Thread.sleep(delay);
      }
    }

    throw new RuntimeException("Timeout after waiting " + watch + " for " + resource);
  }

}
