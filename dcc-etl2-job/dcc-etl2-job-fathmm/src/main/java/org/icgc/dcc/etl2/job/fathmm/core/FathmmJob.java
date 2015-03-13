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

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.job.GenericJob;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.job.fathmm.task.PredictFathmmTask;
import org.icgc.dcc.etl2.job.fathmm.task.ReadTranscriptsTask;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.BiMap;

/**
 * Please see http://fathmm.biocompute.org.uk/
 */
@Slf4j
@Component
public class FathmmJob extends GenericJob {

  @Value("${postgres.url}")
  private String jdbcUrl;

  @Override
  public JobType getType() {
    return JobType.FATHMM;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    val transcripts = readTranscripts(jobContext);
    predict(jobContext, transcripts);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext, FileType.OBSERVATION_FATHMM);
  }

  private BiMap<String, String> readTranscripts(JobContext jobContext) {
    log.info("Reading transcripts...");
    val task = new ReadTranscriptsTask();
    jobContext.execute(task);
    log.info("Finished reading transcripts");

    return task.getTranscripts();
  }

  private void predict(JobContext jobContext, BiMap<String, String> transcripts) {
    jobContext.execute(new PredictFathmmTask(jdbcUrl, transcripts));
  }

}
