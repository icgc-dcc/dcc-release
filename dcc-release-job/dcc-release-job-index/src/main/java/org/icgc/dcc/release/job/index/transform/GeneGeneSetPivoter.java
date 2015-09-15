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
package org.icgc.dcc.release.job.index.transform;

import static com.google.common.collect.Iterables.transform;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getGeneGeneSetId;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getGeneGeneSetType;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getGeneGeneSets;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.removeGeneGeneSets;

import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

@NoArgsConstructor(access = PRIVATE)
public final class GeneGeneSetPivoter {

  /**
   * Constants.
   */
  private static final String GO_TERM_TYPE = "go_term";

  public static Iterable<ObjectNode> pivotGenesGeneSets(@NonNull Iterable<ObjectNode> genes,
      final Map<String, String> geneSetOntologies) {
    // Lazily transform when iteration occurs
    return transform(genes, new Function<ObjectNode, ObjectNode>() {

      @Override
      public ObjectNode apply(ObjectNode gene) {
        return pivotGeneGeneSets(gene, geneSetOntologies);
      }

    });
  }

  private static ObjectNode pivotGeneGeneSets(ObjectNode gene, Map<String, String> geneSetOntologies) {
    val geneGeneSets = getGeneGeneSets(gene);
    if (!geneGeneSets.isMissingNode()) {
      // DonorCentricRowTransform
      for (val geneGeneSet : geneGeneSets) {
        val id = getGeneGeneSetId(geneGeneSet);
        val type = getGeneGeneSetType(geneGeneSet);

        val goTerm = GO_TERM_TYPE.equals(type);
        if (goTerm) {
          val ontology = geneSetOntologies.get(id);
          gene.with(type).withArray(ontology).add(id);
        } else {
          gene.withArray(type).add(id);
        }
      }
    }

    // TODO: This need to only occur if not the "gene" index
    removeGeneGeneSets(gene);

    return gene;
  }

}
