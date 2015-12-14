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

import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;

import java.util.Collection;

import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.Aggregators;
import org.icgc.dcc.release.core.util.Combiners;
import org.icgc.dcc.release.core.util.JacksonFactory;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.function.CreateDonorCentricDocument;
import org.icgc.dcc.release.job.document.model.Donor;
import org.icgc.dcc.release.job.document.model.Occurrence;

import com.google.common.collect.Lists;

public class DonorCentricDocumentTask extends AbstractDocumentTask {

  private final DocumentJobContext indexJobContext;

  public DonorCentricDocumentTask(DocumentJobContext indexJobContext) {
    super(DocumentType.DONOR_CENTRIC_TYPE);
    this.indexJobContext = indexJobContext;
  }

  @Override
  public void execute(TaskContext taskContext) {
    Collection<Occurrence> zeroValue = Lists.newLinkedList();
    val occurrences = readOccurrences(taskContext)
        .mapToPair(keyOccurrence())
        .aggregateByKey(zeroValue, Aggregators::aggregateCollection, Combiners::combineCollections);
    val occurrencePartitions = occurrences.partitions().size();

    val donors = readDonors(taskContext)
        .mapToPair(donor -> tuple(getDonorId(donor), donor));

    val output = donors.leftOuterJoin(occurrences, occurrencePartitions)
        .map(new CreateDonorCentricDocument(indexJobContext));

    writeDonors(taskContext, output);
  }

  private PairFunction<Occurrence, String, Occurrence> keyOccurrence() {
    return o -> {
      String donor_id = o.get_donor_id();
      o.set_donor_id(null);

      return tuple(donor_id, o);
    };
  }

  private void writeDonors(TaskContext taskContext, JavaRDD<Donor> output) {
    writeOutput(taskContext, output, FileType.DONOR_CENTRIC_DOCUMENT, Donor.class);
  }

  private JavaRDD<Occurrence> readOccurrences(TaskContext taskContext) {
    return readObservations(taskContext)
        .map(row -> JacksonFactory.MAPPER.treeToValue(row, Occurrence.class));
  }

}