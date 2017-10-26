package org.icgc.dcc.release.job.id.test.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.icgc.dcc.release.job.id.function.AddSurrogateMutationId;
import org.icgc.dcc.release.job.id.model.MutationEntity;
import org.icgc.dcc.release.job.id.rpc.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.distribution.Version;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_MUTATION_ID;
import static org.icgc.dcc.id.core.Prefixes.MUTATION_ID_PREFIX;


/**
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
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

public class AddSurrogateMutationIdTest {

  private static io.grpc.Server grpc_server;
  private static EmbeddedPostgres postgres;

  private static String sql = "CREATE TABLE mutation_ids \n" +
      "( \n" +
      "  id         BIGSERIAL NOT NULL, \n" +
      "  \n" +
      "  chromosome       VARCHAR(512) NOT NULL, \n" +
      "  chromosome_start VARCHAR(512) NOT NULL, \n" +
      "  chromosome_end   VARCHAR(512) NOT NULL, \n" +
      "  mutation_type    VARCHAR(512) NOT NULL, \n" +
      "  mutation         VARCHAR(512) NOT NULL, \n" +
      "  assembly_version VARCHAR(512) NOT NULL, \n" +
      "\n" +
      "  creation_release VARCHAR(512) NOT NULL, \n" +
      " \n" +
      "  PRIMARY KEY(chromosome, chromosome_start, chromosome_end, mutation_type, mutation, assembly_version) \n" +
      ");";

  private static JavaSparkContext spark;
  private static SQLContext sqlContext;
  private static JdbcTemplate jdbcTemplate;

  @BeforeClass
  @SneakyThrows
  public static void initialize() {
    grpc_server = ServerBuilder.forPort(6565).addService(new InMemoryDataStoreGRpcService()).build();
    grpc_server.start();

    FileUtils.deleteDirectory(new File("/tmp/dcc/release/id/postgresql"));
    postgres = new EmbeddedPostgres(Version.V9_6_5, "/tmp/dcc/release/id/postgresql");
    String url = postgres.start("localhost", 5432, "dcc_id", "sa", "");

    jdbcTemplate = new JdbcTemplate(new DriverManagerDataSource(url /*"jdbc:postgresql://localhost:5432/dcc_id?user=sa&password="*/));

    jdbcTemplate.execute(sql);

    spark = new JavaSparkContext(
        (new SparkConf()).setMaster("local[1]").setAppName("id-job-unit-test")
    );

    sqlContext = new SQLContext(spark);

  }

  @AfterClass
  @SneakyThrows
  public static void tearDown() {
    grpc_server.shutdown();
    postgres.stop();

    FileUtils.deleteDirectory(new File("/tmp/dcc/release/id/postgresql"));
  }

  @Test
  public void flatmap_call_test() {

    List<MutationEntity> data = ImmutableList.of(
        new MutationEntity(
            "chromosome-1",
            "chromosomestart-1",
            "chromosomeend-1",
            "mutation-1",
            "mutationtype-1",
            "assemblyversion-1",
            "",
            "{}"
        ),
        new MutationEntity(
            "chromosome-2",
            "chromosomestart-2",
            "chromosomeend-2",
            "mutation-2",
            "mutationtype-2",
            "assemblyversion-2",
            "",
            "{}"
        )
    );

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565).usePlaintext(true).build();
    MutationIDServiceGrpc.MutationIDServiceBlockingStub blockingStub =  MutationIDServiceGrpc.newBlockingStub(channel);

    AddSurrogateMutationId.FlatmapFunction flatmapFunction = new AddSurrogateMutationId.FlatmapFunction(blockingStub, new ObjectMapper());
    List<ObjectNode> rets =
      flatmapFunction.call(
          rx.Observable.from( sqlContext.createDataFrame(data, MutationEntity.class).collectAsList() )
      ).toList().toBlocking().first();

    Assert.assertTrue(rets.size() == 2);

    ObjectNode node1 = rets.get(0);
    Assert.assertTrue( node1.get(SURROGATE_MUTATION_ID).textValue().equals(MUTATION_ID_PREFIX+"id-0") );

    ObjectNode node2 = rets.get(1);
    Assert.assertTrue( node2.get(SURROGATE_MUTATION_ID).textValue().equals(MUTATION_ID_PREFIX+"id-1") );
  }

  @Test
  public void flatmap_call_test_in_memory_db() {
    List<MutationEntity> data = ImmutableList.of(
        new MutationEntity(
            "chromosome-1",
            "chromosomestart-1",
            "chromosomeend-1",
            "mutation-1",
            "mutationtype-1",
            "assemblyversion-1",
            "",
            "{}"
        ),
        new MutationEntity(
            "chromosome-2",
            "chromosomestart-2",
            "chromosomeend-2",
            "mutation-2",
            "mutationtype-2",
            "assemblyversion-2",
            "",
            "{}"
        )
    );

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565).usePlaintext(true).build();
    MutationIDServiceGrpc.MutationIDServiceBlockingStub blockingStub =  MutationIDServiceGrpc.newBlockingStub(channel);

    AddSurrogateMutationId.FlatmapFunction flatmapFunction = new AddSurrogateMutationId.FlatmapFunction(blockingStub, new ObjectMapper());
    List<ObjectNode> rets =
        flatmapFunction.call(
            rx.Observable.from( sqlContext.createDataFrame(data, MutationEntity.class).collectAsList() )
        ).toList().toBlocking().first();

    Assert.assertTrue(rets.size() == 2);

    rets.stream().forEach(node -> System.out.println(node.toString()));
  }


  private static class InMemoryDataStoreGRpcService extends MutationIDServiceGrpc.MutationIDServiceImplBase {
    @Override
    public void createMutationID(org.icgc.dcc.release.job.id.rpc.CreateMutationIDRequest request,
                                 io.grpc.stub.StreamObserver<org.icgc.dcc.release.job.id.rpc.CreateMutationIDResponse> responseObserver){

      String sql_batch_insert = "insert into mutation_ids (chromosome, chromosome_start, chromosome_end, mutation, mutation_type, assembly_version, creation_release) values (?, ?, ?, ?, ?, ?, ?) returning id;";


      List<CreateMutationIDRequestEntity> list = request.getEntitiesList();

      responseObserver.onNext(
          CreateMutationIDResponse.newBuilder().addAllIds(

              list.stream().map(entityWithIndex -> {
                CreateMutationID entity = entityWithIndex.getEntity();
                String[] args = {entity.getChromosome(), entity.getChromosomeStart(), entity.getChromosomeEnd(), entity.getMutation(), entity.getMutationType(), entity.getAssemblyVersion(), "ICGC26"};

                String serialNo =
                    jdbcTemplate.query(sql_batch_insert, args, resultSet -> {
                      resultSet.next();
                      return resultSet.getString("id");
                    });

                return CreateMutationIDResponseEntity.newBuilder().setIndex(entityWithIndex.getIndex()).setId(serialNo).build();
              }).collect(Collectors.toList())
          ).build()
      );
      responseObserver.onCompleted();

    }
  }

  private static class AssigningIDDirectlyGRpcService extends MutationIDServiceGrpc.MutationIDServiceImplBase {
    @Override
    public void createMutationID(org.icgc.dcc.release.job.id.rpc.CreateMutationIDRequest request,
                                 io.grpc.stub.StreamObserver<org.icgc.dcc.release.job.id.rpc.CreateMutationIDResponse> responseObserver){

      responseObserver.onNext(
        CreateMutationIDResponse.newBuilder().addAllIds(
          request.getEntitiesList().stream().map(entity ->
            CreateMutationIDResponseEntity.newBuilder().setIndex(entity.getIndex()).setId("id-" + entity.getIndex()).build()
          ).collect(Collectors.toList())
        ).build()
      );

      responseObserver.onCompleted();

    }
  }

}
