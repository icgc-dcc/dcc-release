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
package org.icgc.dcc.etl2.job.join.task;

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.etl2.job.join.utils.Tasks.resolveProjectName;

import java.util.Map;

import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.join.model.Donor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

public class ResolveSampleDonorTask extends GenericTask {

  @Getter(lazy = true)
  private final Broadcast<Map<String, Map<String, Donor>>> sampleDonorBroadcast = createBroadcastVariable();
  private final Map<String, Map<String, Donor>> sampleDonorByProject = Maps.newHashMap();
  private JavaSparkContext sparkContext;

  @Override
  public void execute(TaskContext taskContext) {
    // TODO: assign once?
    sparkContext = taskContext.getSparkContext();
    val sampleDonorIds = resolveSampleDonorIds(taskContext);
    val projectName = resolveProjectName(taskContext);
    sampleDonorByProject.put(projectName, sampleDonorIds);
  }

  private Map<String, Donor> resolveSampleDonorIds(TaskContext taskContext) {
    val clinical = parseClinical(taskContext);
    val donors = clinical.collect();

    val sampleDonorIds = Maps.<String, Donor> newHashMap();
    for (val donor : donors) {
      val donorId = donor.get(SURROGATE_DONOR_ID).textValue();

      for (val specimen : donor.withArray(DONOR_SPECIMEN)) {
        for (val sample : specimen.withArray(DONOR_SAMPLE)) {
          val sampleId = textValue(sample, SUBMISSION_ANALYZED_SAMPLE_ID);
          val _specimenId = textValue(specimen, SURROGATE_SPECIMEN_ID);
          val _sampleId = textValue(sample, SURROGATE_SAMPLE_ID);

          sampleDonorIds.put(sampleId, new Donor(donorId, _specimenId, _sampleId));
        }
      }
    }

    return sampleDonorIds;
  }

  private Broadcast<Map<String, Map<String, Donor>>> createBroadcastVariable() {
    return sparkContext.broadcast(sampleDonorByProject);
  }

  private JavaRDD<ObjectNode> parseClinical(TaskContext taskContext) {
    return readInput(taskContext, FileType.CLINICAL);
  }

}
