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
package org.icgc.dcc.etl2.job.join.task;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.etl2.core.util.Tasks.resolveProjectName;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

public class ResolveRawSequenceDataTask extends GenericTask {

  @Getter(lazy = true)
  private final Broadcast<Map<String, JavaRDD<ObjectNode>>> rawSequenceDataBroadcast = createBroadcastVariable();
  private final Map<String, JavaRDD<ObjectNode>> rawSequenceDataByProject = Maps.newHashMap();
  private JavaSparkContext sparkContext;

  @Override
  public void execute(TaskContext taskContext) {
    sparkContext = taskContext.getSparkContext();
    val rawSequenceData = resolveRawSequenceData(taskContext);
    val projectName = resolveProjectName(taskContext);
    rawSequenceDataByProject.put(projectName, rawSequenceData);
  }

  private JavaRDD<ObjectNode> resolveRawSequenceData(TaskContext taskContext) {
    JavaRDD<ObjectNode> resultRdd = null;
    val createRawSeqDataFunction = new CreateRawSequenceDataObject();

    for (val fileType : filterMetaTypes()) {
      val currentRdd = readInput(taskContext, fileType)
          .map(createRawSeqDataFunction);

      if (resultRdd == null) {
        resultRdd = currentRdd;
      } else {
        resultRdd = resultRdd.union(currentRdd);
      }

    }

    return resultRdd.distinct();
  }

  private Broadcast<Map<String, JavaRDD<ObjectNode>>> createBroadcastVariable() {
    return sparkContext.broadcast(rawSequenceDataByProject);
  }

  private static List<FileType> filterMetaTypes() {
    return newArrayList(FileType.values()).stream()
        .filter(ft -> ft.isMetaFileType())
        .collect(toList());

  }

}
