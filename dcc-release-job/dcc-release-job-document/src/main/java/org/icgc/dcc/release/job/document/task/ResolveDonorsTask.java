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
package org.icgc.dcc.release.job.document.task;

import static java.util.stream.Collectors.toMap;
import static org.icgc.dcc.release.core.util.Tasks.resolveProjectName;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;

import java.util.Map;

import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.model.DocumentType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

public class ResolveDonorsTask extends AbstractIndexTask {

  public ResolveDonorsTask(DocumentType type) {
    super(type, DocumentJobContext.builder().build());
  }

  @Getter(lazy = true)
  private final Broadcast<Map<String, ObjectNode>> donorsBroadcast = createBroadcast();
  private Map<String, Map<String, ObjectNode>> donorsByProject = Maps.newHashMap();
  private JavaSparkContext sparkContext;

  @Override
  public void execute(TaskContext taskContext) {
    sparkContext = taskContext.getSparkContext();
    val donorsById = resolveDonors(taskContext);
    val projectName = resolveProjectName(taskContext);
    donorsByProject.put(projectName, donorsById);
  }

  private Map<String, ObjectNode> resolveDonors(TaskContext taskContext) {
    return readDonors(taskContext)
        .mapToPair(project -> tuple(getDonorId(project), project))
        .collectAsMap();
  }

  private Broadcast<Map<String, ObjectNode>> createBroadcast() {
    val donorsById = donorsByProject.entrySet().stream()
        .flatMap(e -> e.getValue().entrySet().stream())
        .collect(toMap(e -> e.getKey(), e -> e.getValue()));

    return sparkContext.broadcast(donorsById);
  }

}
