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

import static org.icgc.dcc.release.core.util.Partitions.getPartitionsCount;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;

import java.util.Collection;

import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.AggregateFunctions;
import org.icgc.dcc.release.core.util.CombineFunctions;
import org.icgc.dcc.release.core.util.Configurations;
import org.icgc.dcc.release.core.util.JavaRDDs;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.function.PairDonor;
import org.icgc.dcc.release.job.document.model.Donor;
import org.icgc.dcc.release.job.document.model.Occurrence;
import org.icgc.dcc.release.job.document.transform.DonorCentricDocumentTransform;

import com.google.common.collect.Lists;

public class DonorCentricDocumentTask extends AbstractDocumentTask {

  private final DocumentJobContext documentJobContext;

  public DonorCentricDocumentTask(@NonNull DocumentJobContext documentJobContext) {
    super(DocumentType.DONOR_CENTRIC_TYPE);
    this.documentJobContext = documentJobContext;
  }

  @Override
  public void execute(TaskContext taskContext) {
    Collection<Occurrence> zeroValue = Lists.newArrayList();
    val occurrences = readOccurrences(taskContext)
        .mapToPair(keyOccurrence())
        .aggregateByKey(zeroValue, AggregateFunctions::aggregateCollection, CombineFunctions::combineCollections);

    val donors = readDonors(taskContext)
        .mapToPair(donor -> tuple(getDonorId(donor), donor));

    val partitionNumbers = getPartitionsCount(occurrences, donors);
    val output = donors.leftOuterJoin(occurrences, partitionNumbers)
        .map(new DonorCentricDocumentTransform(documentJobContext));

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
    val sequenceOutput = output.mapToPair(new PairDonor());
    val outputPath = taskContext.getPath(type.getOutputFileType());
    val conf = Configurations.createJobConf(sequenceOutput);

    JavaRDDs.saveAsSequenceFile(sequenceOutput, Text.class, BytesWritable.class, outputPath, conf);
  }

}