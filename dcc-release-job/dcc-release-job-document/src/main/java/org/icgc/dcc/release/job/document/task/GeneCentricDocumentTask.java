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
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getGeneId;

import java.util.Collection;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.core.util.AggregateFunctions;
import org.icgc.dcc.release.core.util.CombineFunctions;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.function.PairGeneIdObservation;
import org.icgc.dcc.release.job.document.model.Occurrence;
import org.icgc.dcc.release.job.document.transform.GeneCentricDocumentTransform;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class GeneCentricDocumentTask extends AbstractDocumentTask {

  private final DocumentJobContext documentJobContext;

  public GeneCentricDocumentTask(@NonNull DocumentJobContext documentJobContext) {
    super(DocumentType.GENE_CENTRIC_TYPE);
    this.documentJobContext = documentJobContext;
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(TaskContext taskContext) {
    val genes = readGenesPivoted(taskContext);
    val occurrences = readOccurrences(taskContext);
    val output = transform(taskContext, genes, occurrences);
    writeDocOutput(taskContext, output);
  }

  private JavaRDD<Document> transform(TaskContext taskContext, JavaRDD<ObjectNode> genes,
      JavaRDD<Occurrence> occurrences) {
    Collection<Occurrence> zeroValue = Lists.newArrayList();
    val occurrancePairs = occurrences
        .flatMapToPair(new PairGeneIdObservation())
        .aggregateByKey(zeroValue, AggregateFunctions::aggregateCollection, CombineFunctions::combineCollections);

    val partitionsNumber = getPartitionsCount(occurrancePairs, genes);
    val output = genes
        .mapToPair(gene -> tuple(getGeneId(gene), gene))
        .leftOuterJoin(occurrancePairs, partitionsNumber)
        .map(new GeneCentricDocumentTransform(documentJobContext));

    return output;
  }

}
