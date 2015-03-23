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
package org.icgc.dcc.etl2.workflow.util;

import static org.icgc.dcc.etl2.core.util.ObjectNodeRDDs.saveAsTextObjectNodeFile;
import static org.icgc.dcc.etl2.job.imports.util.MongoJavaRDDs.javaMongoCollection;

import java.util.List;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.util.Partitions;
import org.icgc.dcc.etl2.job.imports.config.MongoProperties;
import org.icgc.dcc.etl2.job.imports.util.MongoClientURIBuilder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoConfig;

public class MongoCollectionHDFSImporter {

  @NonNull
  private final String workingDir;
  @NonNull
  private final JavaSparkContext sparkContext;
  @NonNull
  private final MongoProperties properties;
  @NonNull
  private final String database;
  @NonNull
  private final List<String> projectNames;

  public MongoCollectionHDFSImporter(String workingDir, JavaSparkContext sparkContext, MongoProperties properties,
      String database) {
    this.workingDir = workingDir;
    this.sparkContext = sparkContext;
    this.properties = properties;
    this.database = database;
    this.projectNames = getProjectNames();
  }

  public void execute() {
    // Non-partitioned
    execute(ReleaseCollection.RELEASE_COLLECTION, FileType.RELEASE);
    execute(ReleaseCollection.PROJECT_COLLECTION, FileType.PROJECT);
    execute(ReleaseCollection.GENE_COLLECTION, FileType.GENE);
    execute(ReleaseCollection.GENE_SET_COLLECTION, FileType.GENE_SET);
    execute(ReleaseCollection.MUTATION_COLLECTION, FileType.MUTATION);

    // Partitioned
    execute(ReleaseCollection.DONOR_COLLECTION, FileType.DONOR);
    execute(ReleaseCollection.OBSERVATION_COLLECTION, FileType.OBSERVATION);
  }

  @SneakyThrows
  private List<String> getProjectNames() {
    val collectionName = ReleaseCollection.PROJECT_COLLECTION.getId();
    val mongoClient = new MongoClient(getMongoURI(collectionName));
    val db = mongoClient.getDB(database);
    val collection = db.getCollection(collectionName);

    val projectNames = Lists.<String> newArrayList();
    for (val result : collection.find(new BasicDBObject())) {
      val projectName = (String) result.get(LoaderFieldNames.PROJECT_ID);

      projectNames.add(projectName);
    }

    return projectNames;
  }

  private void execute(ReleaseCollection collection, FileType outputFileType) {
    val outputDir = workingDir + "/" + outputFileType.getDirName();
    if (outputFileType.isPartitioned()) {
      for (val projectName : projectNames) {
        val input = readInput(collection.getId(), "{\"" + LoaderFieldNames.PROJECT_ID + "\": \"" + projectName + "\"}");
        saveAsTextObjectNodeFile(input, outputDir + "/" + Partitions.getPartitionName(projectName));
      }
    } else {
      val input = readInput(collection.getId(), "{}");
      saveAsTextObjectNodeFile(input, outputDir);
    }
  }

  private JavaRDD<ObjectNode> readInput(String collection, String query) {
    val hadoopConf = sparkContext.hadoopConfiguration();
    val mongoUri = getMongoURI(collection);

    val mongoConfig = new MongoConfig(hadoopConf);
    mongoConfig.setInputURI(mongoUri.getURI());
    mongoConfig.setSplitSize(properties.getSplitSizeMb());
    mongoConfig.setQuery(query);

    return javaMongoCollection(sparkContext, mongoConfig, new JobConf(hadoopConf));
  }

  private MongoClientURI getMongoURI(String collection) {
    return new MongoClientURIBuilder()
        .uri(properties.getUri())
        .username(properties.getUserName())
        .password(properties.getPassword())
        .database(database)
        .collection(collection)
        .build();
  }

}
