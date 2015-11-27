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

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;
import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.document.BaseDocumentType;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.function.KeyFieldsFunction;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.transform.DonorCentricDocumentTransform;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class DonorCentricIndexTask extends AbstractIndexTask {

  private final DocumentJobContext indexJobContext;

  public DonorCentricIndexTask(DocumentJobContext indexJobContext) {
    super(BaseDocumentType.DONOR_CENTRIC_TYPE, indexJobContext);
    this.indexJobContext = indexJobContext;
  }

  @Override
  public void execute(TaskContext taskContext) {
    val donors = readDonors(taskContext);
    val observations = readObservations(taskContext);
    val donorObservationsPairs = join(donors, observations);

    val output = transform(donorObservationsPairs);

    writeDocOutput(taskContext, output);
  }

  private JavaRDD<Document> transform(JavaPairRDD<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>
      donorObservationsPairs) {
    return donorObservationsPairs.map(new DonorCentricDocumentTransform(indexJobContext));
  }

  private static JavaPairRDD<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> join(
      JavaRDD<ObjectNode> donors,
      JavaRDD<ObjectNode> observations) {
    val donorPairs = donors.mapToPair(donor -> tuple(getDonorId(donor), donor));
    val observationPairs = observations
        .mapToPair(pairByDonorId())
        .groupByKey();
    val donorObservationsPairs = donorPairs.leftOuterJoin(observationPairs);

    return donorObservationsPairs;
  }

  private static KeyFieldsFunction<ObjectNode> pairByDonorId() {
    return new KeyFieldsFunction<ObjectNode>(row -> {
      row.remove(DONOR_ID);
      return row;
    },
        DONOR_ID);
  }

}