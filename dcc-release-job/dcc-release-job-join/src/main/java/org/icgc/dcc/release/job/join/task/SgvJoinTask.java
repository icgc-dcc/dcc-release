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

import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.Map;

import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.SparkWorkaroundUtils;
import org.icgc.dcc.release.job.join.model.DonorSample;
import org.icgc.dcc.release.job.join.model.SgvConsequence;

public class SgvJoinTask extends SecondaryJoinTask {

  public SgvJoinTask(
      Broadcast<Map<String, Map<String, DonorSample>>> donorSamplesbyProject,
      Broadcast<Map<String, Map<String, String>>> sampleSurrogateSampleIdsByProject) {
    super(donorSamplesbyProject, sampleSurrogateSampleIdsByProject, FileType.SGV_P_MASKED);
  }

  @Override
  public void execute(TaskContext taskContext) {
    val primaryMeta = joinPrimaryMeta(taskContext);
    val consequences = readConsequeces(taskContext);

    final Broadcast<Map<String, Iterable<SgvConsequence>>> consequencesBroadcast = taskContext
        .getSparkContext()
        .broadcast(SparkWorkaroundUtils.toHashMap(consequences.collectAsMap()));

    val output = primaryMeta
        .map(new CreateSgvObservation(taskContext.getProjectName().get(), consequencesBroadcast,
            sampleSurrogateSampleIdsByProject));
    writeOutput(taskContext, output, resolveOutputFileType(primaryFileType));
  }

  private JavaPairRDD<String, Iterable<SgvConsequence>> readConsequeces(TaskContext taskContext) {
    val secondaryFileType = resolveSecondaryFileType(primaryFileType);
    val jobConfig = createJobConf(taskContext);

    return readInput(taskContext, jobConfig, secondaryFileType, SgvConsequence.class)
        .distinct()
        .mapToPair(row -> tuple(row.getObservationId(), row))
        .groupByKey();
  }

}
