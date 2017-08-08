package org.icgc.dcc.release.job.id.test.task;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.release.job.id.model.MutationID;
import org.icgc.dcc.release.job.id.task.AddSurrogateMutationIdTask;
import org.icgc.dcc.release.job.id.test.mock.MockIdClientFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.*;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ASSEMBLY_VERSION;
import static org.icgc.dcc.common.core.util.Splitters.TAB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


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

public class AddSurrogateMutationIDTaskTest {

  private static JavaSparkContext jsc;
  private static SQLContext sqlContext;
  private static MockIdClientFactory idClientFactory;
  private static Path tmpRawDataFile;
  private static Path tmpIdDataFile;

  @BeforeClass
  public static void initialize() throws IOException {

    jsc = new JavaSparkContext((new SparkConf()).setMaster("local[4]").setAppName("DCC-RELEASE-ID-Mutation-Test"));
    sqlContext = new SQLContext(jsc);

    tmpRawDataFile = Files.createTempFile("raw_", ".txt");
    Files.deleteIfExists(tmpRawDataFile);

    tmpIdDataFile = Files.createTempFile("id_", ".txt");
    Files.deleteIfExists(tmpIdDataFile);

    Pair<Path, String>[] pairs = new Pair[] {Pair.of(tmpRawDataFile, "/mutation_raw.txt"), Pair.of(tmpIdDataFile, "/mutation_with_id.txt")};
    for(Pair<Path, String> pair: pairs){
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pair.getKey().toFile()))) ;
      BufferedReader reader = new BufferedReader(new InputStreamReader(AddSurrogateMutationIDTaskTest.class.getResourceAsStream(pair.getValue())));
      String line = reader.readLine();
      while(line != null){
        bw.write(line+"\n");
        line = reader.readLine();
      }
      reader.close();
      bw.close();
    }

    idClientFactory = new MockIdClientFactory("", "");
  }

  @AfterClass
  public static void tearDown(){
    jsc.close();
  }

  @Test
  public void test_process(){

    DataFrame ids =
        sqlContext.createDataFrame(
            jsc.textFile(tmpIdDataFile.toFile().getAbsolutePath()).map(row -> {
              List<String> fields = TAB.splitToList(row);
              return new MutationID(
                  fields.get(1),
                  fields.get(2),
                  fields.get(3),
                  fields.get(4),
                  fields.get(5),
                  fields.get(6),
                  fields.get(0)
              );
            }),
            MutationID.class
        ).cache();
    ids.count();

    JavaRDD<ObjectNode> raw =
        jsc.textFile(tmpRawDataFile.toFile().getAbsolutePath()).mapPartitions(
            (FlatMapFunction<Iterator<String>, ObjectNode>) iterator -> {
              Iterable<String> iterable = () -> iterator;
              ObjectMapper mapper = new ObjectMapper();
              return
                  StreamSupport.stream(iterable.spliterator(), false).map(row -> {
                    List<String> fields = TAB.splitToList(row);
                    ObjectNode node = mapper.createObjectNode();
                    node.put(SUBMISSION_OBSERVATION_CHROMOSOME, fields.get(1));
                    node.put(SUBMISSION_OBSERVATION_CHROMOSOME_START, Long.parseLong(fields.get(2)));
                    node.put(SUBMISSION_OBSERVATION_CHROMOSOME_END, Long.parseLong(fields.get(3)));
                    node.put(NORMALIZER_MUTATION, fields.get(4));
                    node.put(SUBMISSION_OBSERVATION_MUTATION_TYPE, fields.get(5));
                    node.put(SUBMISSION_OBSERVATION_ASSEMBLY_VERSION, fields.get(6));
                    node.put("other", "other");
                    node.putNull("testnull");
                    return node;
                  }).collect(Collectors.toList());
            }
        );

    AddSurrogateMutationIdTask task = new AddSurrogateMutationIdTask(idClientFactory, ids, sqlContext);

    try {
      Method method = AddSurrogateMutationIdTask.class.getDeclaredMethod("process", JavaRDD.class);
      method.setAccessible(true);
      JavaRDD<ObjectNode> ret = (JavaRDD<ObjectNode>)method.invoke(task, raw);

      JavaRDD<ObjectNode> cached = ret.cache();

      assertEquals(20, cached.count());

      cached.foreach(node -> System.out.println(node.toString()));

      List<String> collected = cached.map(node -> node.get("_mutation_id").asText()).collect();

      String[] existings = {"MU28719398", "MU16444419", "MU20499025", "MU20499053", "MU16444929", "MU16445007", "MU16445126", "MU16445183", "MU16445215", "MU16445433"};
      for(String id: existings){
        assertTrue(collected.contains(id));
      }

      String[] newIds = {"MU100", "MU101", "MU102", "MU103", "MU104", "MU105", "MU106", "MU107", "MU108", "MU109"};
      for(String id: newIds){
        assertTrue(collected.contains(id));
      }

    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
  }

}
