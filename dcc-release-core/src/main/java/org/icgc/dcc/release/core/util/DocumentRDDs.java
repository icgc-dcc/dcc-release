/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.core.util;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.release.core.util.JacksonFactory.SMILE_READER;
import static org.icgc.dcc.release.core.util.JacksonFactory.SMILE_WRITER;
import static org.icgc.dcc.release.core.util.ObjectNodes.MAPPER;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

@NoArgsConstructor(access = PRIVATE)
public final class DocumentRDDs {

  public static JavaRDD<Document> combineDocumentSequenceFile(@NonNull JavaSparkContext sparkContext,
      @NonNull String paths, @NonNull JobConf conf, @NonNull DocumentType type) {
    return JavaRDDs.combineTextKeySequenceFile(sparkContext, paths, conf)
        .mapToPair(convertToIdAndSource())
        .map(tuple -> new Document(type, tuple._1, tuple._2));
  }

  public static void saveAsTextObjectNodeFile(@NonNull JavaRDD<Document> rdd, @NonNull String path) {
    val output = rdd.map(row -> MAPPER.writeValueAsString(row.getSource()));
    JavaRDDs.saveAsTextFile(output, path);
  }

  public static void saveAsSequenceObjectNodeFile(@NonNull JavaRDD<Document> rdd, @NonNull String path) {
    val conf = Configurations.createJobConf(rdd);
    val pairRdd = rdd
        .mapToPair(row -> tuple(NullWritable.get(), createByteWritable(row.getSource()))
        );

    JavaRDDs.saveAsSequenceFile(pairRdd, NullWritable.class, BytesWritable.class, path, conf);
  }

  public static void saveAsSequenceIdObjectNodeFile(@NonNull JavaRDD<Document> rdd, @NonNull String path) {
    val conf = Configurations.createJobConf(rdd);
    val pairRdd = rdd.mapToPair(DocumentRDDs::pairByDocumentId);

    JavaRDDs.saveAsSequenceFile(pairRdd, Text.class, BytesWritable.class, path, conf);
  }

  private static PairFunction<Tuple2<Text, BytesWritable>, String, ObjectNode> convertToIdAndSource() {
    return tuple -> {
      String documentId = new String(tuple._1.getBytes(), UTF_8);
      ObjectNode value = (ObjectNode) SMILE_READER.readValue(tuple._2.getBytes());

      return tuple(documentId, value);
    };
  }

  private static Tuple2<Text, BytesWritable> pairByDocumentId(Document document) {
    val documentId = document.getId();
    val documentType = document.getType();
    val source = document.getSource();
    val sourceDocumentId = textValue(source, documentType.getPrimaryKey());
    if (!isNullOrEmpty(sourceDocumentId)) {
      checkState(sourceDocumentId.equals(documentId), "Document IDs from key and document don't match. Key document "
          + "ID: '%s'. Source document ID: '%s'", documentId, sourceDocumentId);
    }

    return tuple(new Text(documentId), createByteWritable(source));
  }

  @SneakyThrows
  private static BytesWritable createByteWritable(ObjectNode source) {
    return new BytesWritable(SMILE_WRITER.writeValueAsBytes(source));
  }

}
