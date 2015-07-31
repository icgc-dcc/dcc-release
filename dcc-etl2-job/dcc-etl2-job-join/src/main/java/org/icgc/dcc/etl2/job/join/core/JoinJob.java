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
package org.icgc.dcc.etl2.job.join.core;

import static org.icgc.dcc.etl2.core.job.FileType.CLINICAL;
import static org.icgc.dcc.etl2.core.job.FileType.CNSM;
import static org.icgc.dcc.etl2.core.job.FileType.EXP_ARRAY;
import static org.icgc.dcc.etl2.core.job.FileType.EXP_SEQ;
import static org.icgc.dcc.etl2.core.job.FileType.JCN;
import static org.icgc.dcc.etl2.core.job.FileType.METH_ARRAY;
import static org.icgc.dcc.etl2.core.job.FileType.METH_SEQ;
import static org.icgc.dcc.etl2.core.job.FileType.MIRNA_SEQ;
import static org.icgc.dcc.etl2.core.job.FileType.OBSERVATION;
import static org.icgc.dcc.etl2.core.job.FileType.PEXP;
import static org.icgc.dcc.etl2.core.job.FileType.SGV;
import static org.icgc.dcc.etl2.core.job.FileType.SSM;
import static org.icgc.dcc.etl2.core.job.FileType.STSM;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.job.GenericJob;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.job.join.task.ClinicalJoinTask;
import org.icgc.dcc.etl2.job.join.task.MethArrayJoinTask;
import org.icgc.dcc.etl2.job.join.task.ObservationJoinTask;
import org.icgc.dcc.etl2.job.join.task.PrimaryMetaJoinTask;
import org.icgc.dcc.etl2.job.join.task.ResolveDonorSamplesTask;
import org.icgc.dcc.etl2.job.join.task.ResolveSampleSurrogateSampleIds;
import org.icgc.dcc.etl2.job.join.task.SecondaryJoinTask;
import org.icgc.dcc.etl2.job.join.task.SgvJoinTask;
import org.springframework.stereotype.Component;

@Component
public class JoinJob extends GenericJob {

  @Override
  public JobType getType() {
    return JobType.JOIN;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    join(jobContext);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext, CLINICAL, OBSERVATION, PEXP, JCN, EXP_ARRAY, EXP_SEQ, METH_SEQ, MIRNA_SEQ, SSM, CNSM, STSM,
        SGV, METH_ARRAY);
  }

  private void join(JobContext jobContext) {
    jobContext.execute(new ClinicalJoinTask());

    val resolveDonorSamplesTask = new ResolveDonorSamplesTask();
    val resolveSampleIds = new ResolveSampleSurrogateSampleIds();
    jobContext.execute(resolveDonorSamplesTask, resolveSampleIds);

    val donorSamples = resolveDonorSamplesTask.getDonorSamplesBroadcast();
    val sampleSurrogateSampleIds = resolveSampleIds.getSampleSurrogateSampleIdsBroadcast();
    jobContext.execute(
        new ObservationJoinTask(donorSamples, sampleSurrogateSampleIds),
        new PrimaryMetaJoinTask(donorSamples, FileType.MIRNA_SEQ_P),
        new PrimaryMetaJoinTask(donorSamples, FileType.METH_SEQ_P),
        new PrimaryMetaJoinTask(donorSamples, FileType.EXP_SEQ_P),
        new PrimaryMetaJoinTask(donorSamples, FileType.EXP_ARRAY_P),
        new PrimaryMetaJoinTask(donorSamples, FileType.PEXP_P),
        new PrimaryMetaJoinTask(donorSamples, FileType.JCN_P),
        new SecondaryJoinTask(donorSamples, sampleSurrogateSampleIds, FileType.CNSM_P),
        new SecondaryJoinTask(donorSamples, sampleSurrogateSampleIds, FileType.STSM_P),
        new SgvJoinTask(donorSamples, sampleSurrogateSampleIds),
        new MethArrayJoinTask(donorSamples));
  }

}
