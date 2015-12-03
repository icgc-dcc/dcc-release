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

import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.core.util.Tasks.resolveProjectName;
import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.Map;

import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

/**
 * Creates mapping {@code analyzed_sample_id -> _sample_id}
 */
public class ResolveSampleSurrogateSampleIds extends GenericTask {

  @Getter(lazy = true)
  private final Broadcast<Map<String, Map<String, String>>> sampleSurrogateSampleIdsBroadcast =
      createBroadcastVariable();
  private Map<String, Map<String, String>> sampleSurrogateSampleIdsByProject = Maps.newHashMap();
  private transient JavaSparkContext sparkContext;

  @Override
  public void execute(TaskContext taskContext) {
    sparkContext = taskContext.getSparkContext();
    val sampleIds = resolveSampleIds(taskContext);
    val projectName = resolveProjectName(taskContext);
    sampleSurrogateSampleIdsByProject.put(projectName, sampleIds);
  }

  private JavaRDD<ObjectNode> parseSample(TaskContext taskContext) {
    return readInput(taskContext, FileType.SAMPLE_SURROGATE_KEY);
  }

  private Map<String, String> resolveSampleIds(TaskContext taskContext) {
    val samples = parseSample(taskContext);
    return samples
        .mapToPair(s -> tuple(textValue(s, SUBMISSION_ANALYZED_SAMPLE_ID), textValue(s, SURROGATE_SAMPLE_ID)))
        .collectAsMap();
  }

  private Broadcast<Map<String, Map<String, String>>> createBroadcastVariable() {
    return sparkContext.broadcast(sampleSurrogateSampleIdsByProject);
  }

}
