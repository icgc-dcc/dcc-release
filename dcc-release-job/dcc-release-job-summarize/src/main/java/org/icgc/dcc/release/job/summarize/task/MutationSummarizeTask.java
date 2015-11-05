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
package org.icgc.dcc.release.job.summarize.task;

import static com.google.common.base.Objects.firstNonNull;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_ID;
import static org.icgc.dcc.release.core.job.FileType.MUTATION;
import static org.icgc.dcc.release.core.job.FileType.OBSERVATION_FI;

import java.util.Set;

import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.release.core.function.KeyFieldsFunction;
import org.icgc.dcc.release.core.function.RetainFields;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;

public class MutationSummarizeTask extends GenericTask {

  private static final Set<String> RETAIN_FIELDS = ImmutableSet.of(
      FieldNames.MUTATION_ID,
      FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE,
      FieldNames.SubmissionFieldNames.SUBMISSION_MUTATION,
      FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATION_TYPE,
      FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME,
      FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_START,
      FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_END,
      FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ASSEMBLY_VERSION);

  @Override
  public void execute(TaskContext taskContext) {
    val output = readObservations(taskContext)
        .mapToPair(pairMutationIdMutation())
        .reduceByKey(uniq())
        .map(value());

    writeOutput(taskContext, output, MUTATION);
  }

  private KeyFieldsFunction<ObjectNode> pairMutationIdMutation() {
    val valueFunction = new RetainFields(RETAIN_FIELDS);

    return new KeyFieldsFunction<ObjectNode>(valueFunction, MUTATION_ID);
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  private Function<Tuple2<String, ObjectNode>, ObjectNode> value() {
    return t -> t._2;
  }

  private Function2<ObjectNode, ObjectNode, ObjectNode> uniq() {
    return (a, b) -> {
      return firstNonNull(a, b);
    };
  }

  private JavaRDD<ObjectNode> readObservations(TaskContext taskContext) {
    return readInput(taskContext, OBSERVATION_FI);
  }

}
