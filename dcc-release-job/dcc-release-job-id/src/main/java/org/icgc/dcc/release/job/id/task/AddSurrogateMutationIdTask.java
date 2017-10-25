package org.icgc.dcc.release.job.id.task;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.task.GenericProcessTask;
import org.icgc.dcc.release.job.id.function.AddSurrogateMutationId;
import org.icgc.dcc.release.job.id.model.MutationEntity;
import org.icgc.dcc.release.job.id.model.MutationID;
import scala.collection.convert.WrapAsScala$;

import java.util.Arrays;
import java.util.List;

import static org.icgc.dcc.common.core.util.Splitters.TAB;

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

public class AddSurrogateMutationIdTask extends GenericProcessTask {
  public static final String mutationDumpPath = "/pg_dump/mutation/mutation.txt";
  private DataFrame mutationIDs;
  private SQLContext sqlContext;
  private String remoteServer;
  private int port;

  public AddSurrogateMutationIdTask(String remoteHost, int port, DataFrame df, SQLContext sqlContext) {
    super(FileType.SSM_P_MASKED, FileType.SSM_P_MASKED_SURROGATE_KEY);
    this.mutationIDs = df;
    this.sqlContext = sqlContext;
    this.remoteServer = remoteHost;
    this.port = port;
  }

  @Override
  protected JavaRDD<ObjectNode> process(JavaRDD<ObjectNode> input) {

    DataFrame raw_df =
      sqlContext.createDataFrame(
        input.map(row -> MutationEntity.fromObjectNode(row)),
        MutationEntity.class
      );

    String[] fields = {CONSTANTS.CHROMOSOME, CONSTANTS.CHROMOSOMESTART, CONSTANTS.CHROMOSOMEEND, CONSTANTS.MUTATION, CONSTANTS.MUTATIONTYPE, CONSTANTS.ASSEMBLYVERSION};

    return
        raw_df.join(
            mutationIDs.withColumnRenamed(CONSTANTS.UNIQUEID, CONSTANTS.DBUNIQUEID),
            WrapAsScala$.MODULE$.asScalaBuffer(Arrays.asList(fields)),
            "left_outer"
        ).rdd().toJavaRDD().mapPartitions(new AddSurrogateMutationId(remoteServer, port)).repartition(2);
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

  public static interface CONSTANTS {
    String CHROMOSOME = "chromosome";
    String CHROMOSOMESTART = "chromosomeStart";
    String CHROMOSOMEEND = "chromosomeEnd";
    String MUTATION = "mutation";
    String MUTATIONTYPE = "mutationType";
    String ASSEMBLYVERSION = "assemblyVersion";
    String UNIQUEID = "uniqueId";
    String DBUNIQUEID = "db.uniqueId";
  }
}
