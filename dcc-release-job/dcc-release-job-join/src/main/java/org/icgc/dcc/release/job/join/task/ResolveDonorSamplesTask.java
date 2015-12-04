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
package org.icgc.dcc.release.job.join.task;

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.core.util.Tasks.resolveProjectName;

import java.util.Map;

import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.join.model.DonorSample;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

/**
 * Creates mapping from {@code 'analyzed_sample_id'} to {@code (_donor_id, _specimen_id, _sample_id)}.
 */
public class ResolveDonorSamplesTask extends GenericTask {

  @Getter
  private final Map<String, Map<String, DonorSample>> projectDonorSamples = Maps.newConcurrentMap();

  @Override
  public void execute(TaskContext taskContext) {
    val donorSamples = resolveDonorSamples(taskContext);
    val projectName = resolveProjectName(taskContext);
    projectDonorSamples.put(projectName, donorSamples);
  }

  private Map<String, DonorSample> resolveDonorSamples(TaskContext taskContext) {
    val clinical = parseClinical(taskContext);
    val donors = clinical.collect();

    val donorSamples = Maps.<String, DonorSample> newHashMap();
    for (val donor : donors) {
      val donorId = donor.get(SURROGATE_DONOR_ID).textValue();

      for (val specimen : donor.withArray(DONOR_SPECIMEN)) {
        for (val sample : specimen.withArray(DONOR_SAMPLE)) {
          val sampleId = textValue(sample, SUBMISSION_ANALYZED_SAMPLE_ID);
          val _specimenId = textValue(specimen, SURROGATE_SPECIMEN_ID);
          val _sampleId = textValue(sample, SURROGATE_SAMPLE_ID);

          donorSamples.put(sampleId, new DonorSample(donorId, _specimenId, _sampleId));
        }
      }
    }

    return donorSamples;
  }

  private JavaRDD<ObjectNode> parseClinical(TaskContext taskContext) {
    return readInput(taskContext, FileType.CLINICAL);
  }

}
