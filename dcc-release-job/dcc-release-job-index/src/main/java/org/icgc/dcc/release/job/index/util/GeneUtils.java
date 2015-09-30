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
package org.icgc.dcc.release.job.index.util;

import static org.icgc.dcc.common.core.model.FieldNames.GENE_SET_GO_TERM;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SET_ID;
import static org.icgc.dcc.release.core.util.FieldNames.IndexFieldNames.GO_TERM_ONTOLOGY;
import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.Map;

import lombok.val;
import lombok.experimental.UtilityClass;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.icgc.dcc.release.job.index.function.PivotGeneGeneSets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@UtilityClass
public class GeneUtils {

  private static final String MISSING_VALUE = "";

  public JavaRDD<ObjectNode> pivotGenes(JavaRDD<ObjectNode> genes, JavaRDD<ObjectNode> geneSets) {
    val geneSetIdOntologyPairs = createGeneSetIdOntologyPairs(geneSets);

    return genes.map(new PivotGeneGeneSets(geneSetIdOntologyPairs));
  }

  private Map<String, String> createGeneSetIdOntologyPairs(JavaRDD<ObjectNode> geneSets) {
    return geneSets
        .mapToPair(pairGeneSetOntology())
        .distinct()
        .collectAsMap();

  }

  private PairFunction<ObjectNode, String, String> pairGeneSetOntology() {
    return geneSet -> {
      JsonNode goTerm = geneSet.path(GENE_SET_GO_TERM);

      if (goTerm.isMissingNode()) {
        return tuple(MISSING_VALUE, MISSING_VALUE);
      }

      String id = geneSet.get(GENE_SET_ID).textValue();
      String ontology = goTerm.get(GO_TERM_ONTOLOGY).textValue();

      return tuple(id, ontology);
    };
  }

}
