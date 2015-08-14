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
package org.icgc.dcc.etl2.job.summarize.task;

import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.etl2.core.function.UnwindToPair.unwindToParent;
import static org.icgc.dcc.etl2.core.job.FileType.DONOR_GENE_OBSERVATION_SUMMARY;
import static org.icgc.dcc.etl2.core.job.FileType.OBSERVATION;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.etl2.core.function.string.SelectField;
import org.icgc.dcc.etl2.core.task.GenericProcessTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.summarize.function.CreateDonorSummary;
import org.icgc.dcc.etl2.job.summarize.function.RetainGeneFields;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Getter
public class DonorGeneObservationSummarizeTask extends GenericProcessTask {

  private JavaPairRDD<String, ObjectNode> summary;

  public DonorGeneObservationSummarizeTask() {
    super(OBSERVATION, DONOR_GENE_OBSERVATION_SUMMARY);
  }

  @Override
  public void execute(TaskContext taskContext) {
    val input = readInput(taskContext);
    process(input);
  }

  @Override
  protected JavaRDD<ObjectNode> process(JavaRDD<ObjectNode> input) {
    val keyFunction = new SelectField(OBSERVATION_DONOR_ID);
    val flatFunction = unwindToParent(OBSERVATION_CONSEQUENCES, keyFunction, new RetainGeneFields());
    summary = input
        .flatMapToPair(flatFunction)
        .filter(filterSsm())
        .distinct()
        .groupByKey()
        .mapToPair(new CreateDonorSummary());

    return null;
  }

  private static Function<Tuple2<String, ObjectNode>, Boolean> filterSsm() {
    return tuple -> textValue(tuple._2, OBSERVATION_TYPE).equals(SSM_TYPE.getId());
  }

}