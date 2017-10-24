package org.icgc.dcc.release.job.id.mock.rpc;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.apache.commons.lang3.tuple.Pair;
import org.icgc.dcc.release.job.id.config.PostgresqlProperties;
import org.icgc.dcc.release.job.id.rpc.*;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

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
@GrpcService(MutationIDServiceGrpc.class)
@RequiredArgsConstructor
@Slf4j
public class MutationIDService extends MutationIDServiceGrpc.MutationIDServiceImplBase {

  @NonNull
  private DriverManagerDataSource dataSource;

  private String table_name = "tmp_mutation_ids";
  private String sql_batch_insert = "insert into " + table_name + " (chromosome, chromosome_start, chromosome_end, mutation, mutation_type, assembly_version, creation_release) values (?, ?, ?, ?, ?, ?, ?) returning id;";

  @Override
  public void createMutationID(org.icgc.dcc.release.job.id.rpc.CreateMutationIDRequest request,
                               io.grpc.stub.StreamObserver<org.icgc.dcc.release.job.id.rpc.CreateMutationIDResponse> responseObserver) {

    List<CreateMutationIDRequestEntity> list = request.getEntitiesList();

    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

    responseObserver.onNext(
      CreateMutationIDResponse.newBuilder().addAllIds(

        list.stream().map(entityWithIndex -> {
          CreateMutationID entity = entityWithIndex.getEntity();
          Object[] args = {entity.getChromosome(), entity.getChromosomeStart(), entity.getChromosomeEnd(), entity.getMutation(), entity.getMutationType(), entity.getAssemblyVersion(), "ICGC26"};
          String serialNo =
            jdbcTemplate.query(sql_batch_insert, args, resultSet -> {
              return resultSet.getString("id");
            });
          return CreateMutationIDResponseEntity.newBuilder().setIndex(entityWithIndex.getIndex()).setId(serialNo).build();
        }).collect(Collectors.toList())
      ).build()
    );
    responseObserver.onCompleted();


  }
}
