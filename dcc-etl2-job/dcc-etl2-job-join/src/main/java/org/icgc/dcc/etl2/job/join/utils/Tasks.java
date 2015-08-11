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
package org.icgc.dcc.etl2.job.join.utils;

import static java.util.Collections.emptyMap;
import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.join.model.DonorSample;

@NoArgsConstructor(access = PRIVATE)
public class Tasks {

  public static final String NO_PROJECTS = "";

  @NonNull
  public static String resolveProjectName(TaskContext taskContext) {
    val project = taskContext.getProjectName();

    return project.isPresent() ? project.get() : NO_PROJECTS;
  }

  @NonNull
  public static Map<String, DonorSample> resolveDonorSamples(TaskContext taskContext,
      Broadcast<Map<String, Map<String, DonorSample>>> broadcast) {
    val projectName = resolveProjectName(taskContext);
    val result = broadcast.value().get(projectName);

    return result == null ? emptyMap() : result;
  }

  public static Map<String, String> getSampleSurrogateSampleIds(TaskContext taskContext,
      Broadcast<Map<String, Map<String, String>>> broadcast) {
    val projectName = resolveProjectName(taskContext);
    val result = broadcast.value().get(projectName);

    return result == null ? emptyMap() : result;
  }

}
