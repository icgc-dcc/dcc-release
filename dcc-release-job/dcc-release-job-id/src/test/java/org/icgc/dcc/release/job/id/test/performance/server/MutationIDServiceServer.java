package org.icgc.dcc.release.job.id.test.performance.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.icgc.dcc.release.job.id.mock.rpc.MutationIDService;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

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
public class MutationIDServiceServer {

  @NonNull
  DriverManagerDataSource dataSource;

  @SneakyThrows
  public void start() {
    System.out.println("creating server ...");
    Server server = ServerBuilder.forPort(6565).addService(new MutationIDService(dataSource)).build();

    System.out.println("before starting ...");
    server.start();

    server.awaitTermination();
  }

  public static void main(String[] args) {

    new MutationIDServiceServer(
        new DriverManagerDataSource(
            "jdbc:postgresql://localhost:5432/dcc_id?user=postgres&password=wuqiGXX57"
        )
    ).start();

  }
}
