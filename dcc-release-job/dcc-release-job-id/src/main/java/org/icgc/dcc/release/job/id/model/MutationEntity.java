package org.icgc.dcc.release.job.id.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.*;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;


/**
 * Copyright (c) $today.year The Ontario Institute for Cancer Research. All rights reserved.
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
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MutationEntity {

  private static final String ASSEMBLY_VERSION = "GRCh37";

  @NonNull
  private  String chromosome;

  @NonNull
  private  String chromosomeStart;

  @NonNull
  private  String chromosomeEnd;

  @NonNull
  private  String mutation;

  @NonNull
  private  String mutationType;

  @NonNull
  private  String assemblyVersion;

  @NonNull
  private  String uniqueId;

  @NonNull
  private String all;


  public static MutationEntity fromObjectNode(ObjectNode node) {

    MutationEntity entity = new MutationEntity();
    entity.chromosome = node.get(SUBMISSION_OBSERVATION_CHROMOSOME).textValue();
    entity.chromosomeStart = textValue(node, SUBMISSION_OBSERVATION_CHROMOSOME_START);
    entity.chromosomeEnd = textValue(node, SUBMISSION_OBSERVATION_CHROMOSOME_END);
    entity.mutation = node.get(NORMALIZER_MUTATION).textValue();
    entity.mutationType = node.get(SUBMISSION_OBSERVATION_MUTATION_TYPE).textValue();
    entity.assemblyVersion = ASSEMBLY_VERSION;
    entity.uniqueId = "";
    entity.all = node.toString();

    return entity;
  }
}
