package org.icgc.dcc.release.job.id.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.icgc.dcc.release.job.id.rpc.*;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_MUTATION_ID;
import static org.icgc.dcc.id.core.Prefixes.MUTATION_ID_PREFIX;
import static org.icgc.dcc.release.job.id.task.AddSurrogateMutationIdTask.CONSTANTS.*;

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

@RequiredArgsConstructor
public class AddSurrogateMutationId implements FlatMapFunction<Iterator<Row>, ObjectNode>{

  @NonNull
  private String remoteServer;
  @NonNull
  private int port;

  @Override
  public Iterable<ObjectNode> call(Iterator<Row> iterator) throws Exception {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(remoteServer, port).usePlaintext(true).build();
    MutationIDServiceGrpc.MutationIDServiceBlockingStub blockingStub =  MutationIDServiceGrpc.newBlockingStub(channel);

    ObjectMapper mapper = new ObjectMapper();

    return

        Observable.from(() -> iterator).groupBy(row -> {
          String uniqueId = row.getAs(DBUNIQUEID);
          if(uniqueId == null || uniqueId.isEmpty())
            return "new";
          else
            return "old";
        }).flatMap(group -> {
          if(group.getKey().equals("old")){
            return
                group.map(existingRow -> {
                  try {
                    ObjectNode node = (ObjectNode)mapper.readTree(existingRow.<String>getAs("all"));
                    node.put(SURROGATE_MUTATION_ID, MUTATION_ID_PREFIX + existingRow.<String>getAs(DBUNIQUEID));
                    return node;
                  } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                  }
                });
          }
          else{
            return
                group.window(10000).flatMap(
                    new FlatmapFunction(blockingStub, mapper)
                ); //end of group.window(10000).map
          }//end of else
        }).toList().toBlocking().first();
  }

  @RequiredArgsConstructor
  public class FlatmapFunction implements Func1<Observable<Row>, Observable<ObjectNode> >{

    @NonNull
    private MutationIDServiceGrpc.MutationIDServiceBlockingStub blockingStub;
    @NonNull
    private ObjectMapper mapper;

    @Override
    public Observable<ObjectNode> call(Observable<Row> batch) {
      return
          batch.toList().flatMap(list -> {

            AtomicInteger index = new AtomicInteger(0);

            CreateMutationIDRequest request =
                CreateMutationIDRequest.newBuilder().addAllEntities(
                    list.stream().map(row ->
                        CreateMutationIDRequestEntity.newBuilder()
                            .setIndex(index.getAndIncrement())
                            .setEntity(
                                CreateMutationID.newBuilder()
                                    .setChromosome(row.<String>getAs(CHROMOSOME))
                                    .setChromosomeStart(row.<String>getAs(CHROMOSOMESTART))
                                    .setChromosomeEnd(row.<String>getAs(CHROMOSOMEEND))
                                    .setMutation(row.<String>getAs(MUTATION))
                                    .setMutationType(row.<String>getAs(MUTATIONTYPE))
                                    .setAssemblyVersion(row.<String>getAs(ASSEMBLYVERSION)).build()
                            ).build()
                    ).collect(Collectors.toList())
                ).build();

            List<CreateMutationIDResponseEntity> rets = blockingStub.createMutationID(request).getIdsList();

            return
                Observable.from(
                    IntStream.range(0, list.size()).<ObjectNode>mapToObj(index_ -> {
                      try {
                        ObjectNode node = (ObjectNode)mapper.readTree(list.get(index_).<String>getAs("all"));
                        node.put(SURROGATE_MUTATION_ID, MUTATION_ID_PREFIX + rets.get(index_).getId());
                        return node;
                      } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                      }
                    }).collect(Collectors.toList())
                );

          });
    }
  }
}
