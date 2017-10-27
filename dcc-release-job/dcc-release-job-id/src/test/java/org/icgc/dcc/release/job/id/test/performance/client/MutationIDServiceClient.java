package org.icgc.dcc.release.job.id.test.performance.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.avro.generic.GenericData;
import org.icgc.dcc.release.job.id.rpc.*;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.*;

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

public class MutationIDServiceClient {
  public static void main(String[] args) {

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565).usePlaintext(true).build();
    MutationIDServiceGrpc.MutationIDServiceBlockingStub blockingStub =  MutationIDServiceGrpc.newBlockingStub(channel);

    long start = System.nanoTime();

    Observable.range(0, 1000000).window(10000).flatMap(group ->

      group.map(i ->
            CreateMutationIDRequestEntity.newBuilder().setEntity(
                CreateMutationID.newBuilder()
                    .setChromosome("chromosome-" + i)
                    .setChromosomeStart("chromosomestart-" + i)
                    .setChromosomeEnd("chromosomeend-" + i)
                    .setMutation("mutation-" + i)
                    .setMutationType("mutationtype-" + i)
                    .setAssemblyVersion("assemblyversion-" + i)
                    .build()
            ).setIndex(i).build()
      ).toList()

    ).subscribe(
        group -> {
          blockingStub.createMutationID(
              CreateMutationIDRequest.newBuilder().addAllEntities(group).build()
          );
        }
    );

    long end = System.nanoTime();

    System.out.println("Finish handling 1M items, takes " + (end - start) );

  }

}
