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
package org.icgc.dcc.release.job.join.task;

import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.ARRAY_PLATFORM;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.PROBE_ID;
import static org.icgc.dcc.release.core.util.Keys.getKey;

import java.util.Map;

import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.function.KeyFields;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.SparkWorkaroundUtils;
import org.icgc.dcc.release.job.join.model.DonorSample;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class MethArrayJoinTask extends PrimaryMetaJoinTask {

  private static final FileType PRIMARY_FILE_TYPE = FileType.METH_ARRAY_P;
  private static final String[] PROBE_JOIN_KEYS = { ARRAY_PLATFORM, PROBE_ID };

  public MethArrayJoinTask(Broadcast<Map<String, Map<String, DonorSample>>> donorSamplesByProject) {
    super(donorSamplesByProject, PRIMARY_FILE_TYPE);
  }

  @Override
  public void execute(TaskContext taskContext) {
    val primaryMeta = joinPrimaryMeta(taskContext);
    val probes = resolveProbes(taskContext);
    val output = joinPrimaryMetaProbes(primaryMeta, probes);

    writeOutput(taskContext, output, FileType.METH_ARRAY);
  }

  private Broadcast<Map<String, ObjectNode>> resolveProbes(TaskContext taskContext) {
    val probes = readInput(taskContext, FileType.METH_ARRAY_PROBES)
        .mapToPair(new KeyFields(PROBE_JOIN_KEYS))
        .collectAsMap();
    val sparkContext = taskContext.getSparkContext();

    return sparkContext.broadcast(SparkWorkaroundUtils.toHashMap(probes));
  }

  private static JavaRDD<ObjectNode> joinPrimaryMetaProbes(JavaRDD<ObjectNode> primaryMeta,
      Broadcast<Map<String, ObjectNode>> probes) {
    return primaryMeta.map(joinProbes(probes));
  }

  private static Function<ObjectNode, ObjectNode> joinProbes(Broadcast<Map<String, ObjectNode>> probes) {
    return row -> {
      String key = getKey(row, PROBE_JOIN_KEYS);
      if (key == null) {
        return row;
      }

      ObjectNode probe = probes.value().get(key);
      if (probe != null) {
        row.setAll(probe);
      }

      return row;
    };
  }

}
