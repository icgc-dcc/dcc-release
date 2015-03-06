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
package org.icgc.dcc.etl2.job.fathmm.core;

import java.util.concurrent.atomic.AtomicReference;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.job.Job;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.core.task.GenericProcessTask;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskExecutor;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.fathmm.function.PredictFathmm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;

/**
 * Please see http://fathmm.biocompute.org.uk/
 */
@Component
public class FathmmJob implements Job {

  /**
   * Dependencies.
   */
  @Autowired
  private TaskExecutor executor;

  @Value("${postgres.url}")
  private String jdbcUrl;

  @Override
  public JobType getJobType() {
    return JobType.FATHMM;
  }

  @Override
  @SneakyThrows
  public void execute(@NonNull JobContext jobContext) {
    val transcripts = readTranscripts(jobContext);

    predict(jobContext, transcripts);
  }

  private BiMap<String, String> readTranscripts(JobContext jobContext) {
    val result = new AtomicReference<BiMap<String, String>>();

    executor.execute(jobContext, new GenericTask("transcripts") {

      @Override
      public void execute(TaskContext taskContext) {
        val transcripts = new FathmmTranscriptReader(taskContext).readTranscripts();

        result.set(transcripts);
      }

    });

    return result.get();
  }

  private void predict(JobContext jobContext, BiMap<String, String> transcripts) {
    executor.execute(jobContext, new GenericProcessTask(FileType.OBSERVATION, FileType.OBSERVATION_FATHMM) {

      @Override
      protected JavaRDD<ObjectNode> process(JavaRDD<ObjectNode> input) {
        return input.map(new PredictFathmm(jdbcUrl, transcripts));
      }

    });
  }

}
