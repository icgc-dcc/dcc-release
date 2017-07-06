package org.icgc.dcc.release.job.id.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.icgc.dcc.id.client.core.IdClient;
import org.icgc.dcc.id.client.core.IdClientFactory;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.job.id.model.MutationEntity;
import org.icgc.dcc.release.job.id.model.MutationID;
import rx.Observable;
import scala.collection.convert.WrapAsScala$;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.*;
import static org.icgc.dcc.common.core.util.Splitters.TAB;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

/**
 * Copyright (c) $today.year The Ontario Institute for Cancer Research. All rights reserved.
 * <p>
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 * <p>
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

public class AddSurrogateMutationIdTask extends AddSurrogateIdTask {
  private static final String MUTATION_ID_PREFIX = "MU";
  public static final String mutationDumpPath = "/pg_dump/mutation/mutation.txt";
  private DataFrame mutationIDs;
  private SQLContext sqlContext;

  public AddSurrogateMutationIdTask(@NonNull IdClientFactory idClientFactory, DataFrame df, SQLContext sqlContext) {
    super(FileType.SSM_P_MASKED, FileType.SSM_P_MASKED_SURROGATE_KEY, idClientFactory);
    this.mutationIDs = df;
    this.sqlContext = sqlContext;
  }

  @Override
  protected JavaRDD<ObjectNode> process(JavaRDD<ObjectNode> input) {

    IdClientFactory localIdClientFactory = idClientFactory;

    DataFrame raw_df =
      sqlContext.createDataFrame(
        input.map(row -> MutationEntity.fromObjectNode(row)),
        MutationEntity.class
      );

    String[] fields = {"chromosome", "chromosomeStart", "chromosomeEnd", "mutation", "mutationType", "assemblyVersion"};

    return
        raw_df.join(
            mutationIDs.withColumnRenamed("uniqueId", "db.uniqueId"),
            WrapAsScala$.MODULE$.asScalaBuffer(Arrays.asList(fields)),
            "left_outer"
        ).rdd().toJavaRDD().mapPartitions(iterator -> {

          IdClient idClient = localIdClientFactory.create();
          ObjectMapper mapper = new ObjectMapper();

          Iterable<Row> iterable = () -> iterator;
          return
            StreamSupport.stream(iterable.spliterator(), false).map(row -> {
              try {
                ObjectNode node = (ObjectNode)mapper.readTree(row.<String>getAs("rest"));

                String chromosome = row.<String>getAs("chromosome");
                String chromosomeStart = row.<String>getAs("chromosomeStart");
                String chromosomeEnd = row.<String>getAs("chromosomeEnd");
                String mutation = row.<String>getAs("mutation");
                String mutationType = row.<String>getAs("mutationType");
                String assemblyVersion = row.<String>getAs("assemblyVersion");

                node.put(SUBMISSION_OBSERVATION_CHROMOSOME, chromosome);
                node.put(SUBMISSION_OBSERVATION_CHROMOSOME_START, Long.parseLong(chromosomeStart));
                node.put(SUBMISSION_OBSERVATION_CHROMOSOME_END, Long.parseLong(chromosomeEnd));
                node.put(NORMALIZER_MUTATION, mutation);
                node.put(SUBMISSION_OBSERVATION_MUTATION_TYPE, mutationType);

                String uniqueId = row.<String>getAs("db.uniqueId");
                if(uniqueId == null || uniqueId.isEmpty()){
                  node.put(SURROGATE_MUTATION_ID,
                      idClient.createMutationId(
                          chromosome,
                          chromosomeStart,
                          chromosomeEnd,
                          mutation,
                          mutationType,
                          assemblyVersion
                      )
                  );
                }
                else{
                  node.put(SURROGATE_MUTATION_ID, MUTATION_ID_PREFIX + row.<String>getAs("db.uniqueId"));
                }

                return node;

              } catch (IOException e) {
                e.printStackTrace();
                return null;
              }
            }).collect(Collectors.toList());

        });
  }

  public static DataFrame createDataFrameForPGData(SQLContext sqlContext, JobContext jobContext, String dumpPath) {
    DataFrame mutationDF =
        sqlContext.createDataFrame(
            jobContext.getJavaSparkContext().textFile(jobContext.getFileSystem().getConf().get("fs.defaultFS") + jobContext.getWorkingDir() + dumpPath, 10).map(row -> {
              List<String> fields =
                  TAB.trimResults().omitEmptyStrings().splitToList(row);
              return new MutationID(fields.get(1), fields.get(2), fields.get(3), fields.get(4), fields.get(5), fields.get(6), fields.get(0));
            }),
            MutationID.class
        ).cache();
    mutationDF.count();
    return mutationDF;
  }
}
