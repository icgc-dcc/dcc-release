/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl2.job.index.transform;

import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_GENE_ID;
import static org.icgc.dcc.etl2.job.index.util.JsonNodes.normalizeTextValue;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl2.job.index.core.DocumentTransform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractCentricDocumentTransform implements DocumentTransform {

  protected static void transformGeneObservationConsequences(@NonNull ObjectNode gene,
      @NonNull ObjectNode geneObservation) {
    // Shorthands
    val geneId = gene.path(GENE_ID).textValue();
    val consequences = (ArrayNode) geneObservation.get(OBSERVATION_CONSEQUENCES);

    if (consequences != null) {
      // Remove consequences unrelated to the target gene
      val iterator = consequences.iterator();
      while (iterator.hasNext()) {
        JsonNode consequence = iterator.next();

        // Normalize
        String consequenceGeneId = normalizeTextValue(consequence, OBSERVATION_CONSEQUENCES_GENE_ID);

        boolean related =
            geneId == null && consequenceGeneId == null || geneId != null && geneId.equals(consequenceGeneId);

        if (!related) {
          iterator.remove();
        }
      }
    }
  }

}
