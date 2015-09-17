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
package org.icgc.dcc.release.job.id.function;

import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_END;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_START;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATION_TYPE;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import lombok.val;

import org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames;
import org.icgc.dcc.id.client.core.IdClientFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class AddSurrogateMutationId extends AddSurrogateId {

  private static final String ASSEMBLY_VERSION = "GRCh37";

  public AddSurrogateMutationId(IdClientFactory idClientFactory) {
    super(idClientFactory);
  }

  @Override
  public ObjectNode call(ObjectNode row) throws Exception {
    val chromosome = row.get(SUBMISSION_OBSERVATION_CHROMOSOME).textValue();
    val chromosomeStart = textValue(row, SUBMISSION_OBSERVATION_CHROMOSOME_START);
    val chromosomeEnd = textValue(row, SUBMISSION_OBSERVATION_CHROMOSOME_END);
    val mutation = row.get(NORMALIZER_MUTATION).textValue();
    val mutationType = row.get(SUBMISSION_OBSERVATION_MUTATION_TYPE).textValue();
    // String assemblyVersion = row.get(SUBMISSION_OBSERVATION_ASSEMBLY_VERSION).textValue();

    // TODO: get from meta file
    String assemblyVersion = ASSEMBLY_VERSION;

    val mutationId = client()
        .getMutationId(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion)
        .get();

    row.put(IdentifierFieldNames.SURROGATE_MUTATION_ID, mutationId);

    return row;
  }
}