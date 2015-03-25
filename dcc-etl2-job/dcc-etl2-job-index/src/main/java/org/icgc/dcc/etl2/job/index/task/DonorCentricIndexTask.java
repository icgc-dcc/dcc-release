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

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.etl2.core.function.FilterFields;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.index.core.CollectionReader;
import org.icgc.dcc.etl2.job.index.core.Document;
import org.icgc.dcc.etl2.job.index.core.DocumentContext;
import org.icgc.dcc.etl2.job.index.io.HDFSCollectionReader;
import org.icgc.dcc.etl2.job.index.model.CollectionFields;
import org.icgc.dcc.etl2.job.index.model.DocumentType;
import org.icgc.dcc.etl2.job.index.util.CollectionFieldsFilterAdapter;
import org.icgc.dcc.etl2.job.index.util.DefaultDocumentContext;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class DonorCentricIndexTask extends GenericTask {

  private final DocumentType type;

  public DonorCentricIndexTask() {
    super(DocumentType.DONOR_CENTRIC_TYPE.getName());
    this.type = DocumentType.DONOR_CENTRIC_TYPE;
  }

  @Override
  public void execute(TaskContext taskContext) {
    val donors = readDonors(taskContext);
    val observations = readObservations(taskContext);

    val donorPairs = donors.mapToPair(donor -> pair(getDonorId(donor), donor));
    val observationPairs = observations.groupBy(observation -> getObservationDonorId(observation));

    val donorObservationsPairs = donorPairs.leftOuterJoin(observationPairs);

    donorObservationsPairs.map(createTransform(taskContext)).count();
  }

  private JavaRDD<ObjectNode> readDonors(TaskContext taskContext) {
    val fields = type.getFields().getDonorFields();
    return filter(readInput(taskContext, FileType.DONOR), fields);
  }

  private JavaRDD<ObjectNode> readObservations(TaskContext taskContext) {
    val fields = type.getFields().getObservationFields();
    return filter(readInput(taskContext, FileType.OBSERVATION), fields);
  }

  private Transform createTransform(TaskContext taskContext) {
    val collectionDir = taskContext.getJobContext().getWorkingDir();
    val fsUri = taskContext.getFileSystem().getUri();

    return new Transform(type, collectionDir, fsUri);
  }

  private static JavaRDD<ObjectNode> filter(JavaRDD<ObjectNode> rdd, CollectionFields fields) {
    return rdd.map(new FilterFields(new CollectionFieldsFilterAdapter(fields)));
  }

  private static Tuple2<String, ObjectNode> pair(String id, ObjectNode row) {
    return new Tuple2<String, ObjectNode>(id, row);
  }

  @RequiredArgsConstructor
  private static class Transform implements
      Function<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>, ObjectNode> {

    private final DocumentType type;
    private final String collectionDir;
    private final URI fsUri;

    private CollectionReader collectionReader;

    @Override
    public ObjectNode call(Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) throws Exception {
      val donor = tuple._2._1;
      val donorObservations = tuple._2._2;

      val context = createDocumentContext(donorObservations);
      val document = transform(donor, context);

      return document.getSource();
    }

    private Document transform(ObjectNode donor, DocumentContext context) {
      return type.getTransform().transformDocument(donor, context);
    }

    private DocumentContext createDocumentContext(Optional<Iterable<ObjectNode>> donorObservations) throws IOException {
      return new DefaultDocumentContext("indexName", type, getCollectionReader()) {

        @Override
        public Iterable<ObjectNode> getObservationsByDonorId(String donorId) {
          return donorObservations.or(Collections.emptyList());
        }

      };
    }

    private CollectionReader getCollectionReader() throws IOException {
      if (collectionReader != null) {
        val fileSystem = FileSystem.get(fsUri, new Configuration());
        collectionReader = new HDFSCollectionReader(new Path(collectionDir), fileSystem);

      }

      return collectionReader;
    }

  }

}