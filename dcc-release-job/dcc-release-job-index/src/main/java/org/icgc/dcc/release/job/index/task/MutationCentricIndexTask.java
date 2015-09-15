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
package org.icgc.dcc.release.job.index.task;

import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getMutationId;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getObservationMutationId;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.job.index.function.MutationCentricRowTransform;
import org.icgc.dcc.release.job.index.model.DocumentType;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class MutationCentricIndexTask extends IndexTask {

  public MutationCentricIndexTask() {
    super(DocumentType.MUTATION_CENTRIC_TYPE);
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(TaskContext taskContext) {
    val mutations = readMutations(taskContext);
    val observations = readObservations(taskContext);

    val output = transform(taskContext, mutations, observations);
    writeOutput(output);
  }

  private JavaRDD<ObjectNode> transform(TaskContext taskContext,
      JavaRDD<ObjectNode> mutations, JavaRDD<ObjectNode> observations) {
    val mutationPairs = mutations.mapToPair(mutation -> pair(getMutationId(mutation), mutation));
    val observationPairs = observations.groupBy(observation -> getObservationMutationId(observation));

    val mutationObservationsPairs = mutationPairs.leftOuterJoin(observationPairs);
    val transformed = mutationObservationsPairs.map(createTransform(taskContext));

    return transformed;
  }

  private MutationCentricRowTransform createTransform(TaskContext taskContext) {
    val collectionDir = taskContext.getJobContext().getWorkingDir();
    val fsUri = taskContext.getFileSystem().getUri();

    return new MutationCentricRowTransform(collectionDir, fsUri);
  }

}
