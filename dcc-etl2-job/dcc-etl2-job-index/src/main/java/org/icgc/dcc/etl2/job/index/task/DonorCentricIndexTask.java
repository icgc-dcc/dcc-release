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
package org.icgc.dcc.etl2.job.index.task;

import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getDonorId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationDonorId;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.index.function.DonorCentricRowTransform;
import org.icgc.dcc.etl2.job.index.function.RowTransform;
import org.icgc.dcc.etl2.job.index.model.DocumentType;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class DonorCentricIndexTask extends IndexTask {

  public DonorCentricIndexTask() {
    super(DocumentType.DONOR_CENTRIC_TYPE);
  }

  @Override
  public void execute(TaskContext taskContext) {
    val donors = readDonors(taskContext);
    val observations = readObservations(taskContext);

    val output = transform(taskContext, donors, observations);
    writeOutput(output, "indexer");
  }

  private JavaRDD<ObjectNode> transform(TaskContext taskContext,
      JavaRDD<ObjectNode> donors, JavaRDD<ObjectNode> observations) {
    val donorPairs = donors.mapToPair(donor -> pair(getDonorId(donor), donor));
    val observationPairs = observations.groupBy(observation -> getObservationDonorId(observation));
    val donorObservationsPairs = donorPairs.leftOuterJoin(observationPairs);
    val transformed = donorObservationsPairs.map(createTransform(taskContext));

    return transformed;
  }

  private RowTransform createTransform(TaskContext taskContext) {
    val collectionDir = taskContext.getJobContext().getWorkingDir();
    val fsUri = taskContext.getFileSystem().getUri();

    return new DonorCentricRowTransform(collectionDir, fsUri);
  }

}