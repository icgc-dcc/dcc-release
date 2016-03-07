/*
q * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.document.util;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_GENE_ID;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.isBlank;

import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

/**
 * The concept of a "fake" is an implementation technique to provide "Null Object" semantics to document nodes and
 * ensure nesting from this surrogate is possible.
 * 
 * @see http://en.wikipedia.org/wiki/Proxy_pattern
 * @see http://en.wikipedia.org/wiki/Null_Object_pattern
 */
@NoArgsConstructor(access = PRIVATE)
public final class Fakes {

  /**
   * Boolean property of an array element that will be {@code true} for a "placeholder" element.
   */
  public static final String PLACEHOLDER_FLAG = "placeholder";

  /**
   * ID of "fake" gene
   */
  public static final String FAKE_GENE_ID = "";

  /**
   * Boolean property of gene that will be {@code true} for a "fake" gene.
   */
  public static final String GENE_FAKE_FLAG = "fake";

  /**
   * ID of "fake" transcript
   */
  public static final String FAKE_TRANSCRIPT_ID = "";

  /**
   * Boolean property of gene that will be {@code true} for a "fake" transcript.
   */
  public static final String TRANSCRIPT_FAKE_FLAG = "fake";

  /**
   * Constants.
   */
  private static final ObjectMapper CREATION_MAPPER = new ObjectMapper();

  public static ObjectNode createPlaceholder() {
    return CREATION_MAPPER.createObjectNode().put(PLACEHOLDER_FLAG, true);
  }

  public static boolean isPlaceholder(@NonNull JsonNode jsonNode) {
    return jsonNode.isObject() && jsonNode.path(PLACEHOLDER_FLAG).asBoolean();
  }

  public static boolean isFakeGeneId(String geneId) {
    return FAKE_GENE_ID.equals(geneId);
  }

  public static ObjectNode createFakeGene() {
    return CREATION_MAPPER.createObjectNode().put(GENE_FAKE_FLAG, true);
  }

  public static boolean isFakeTranscriptId(String transcriptId) {
    return FAKE_TRANSCRIPT_ID.equals(transcriptId);
  }

  public static ObjectNode createFakeTranscript() {
    return CREATION_MAPPER.createObjectNode().put(TRANSCRIPT_FAKE_FLAG, true);
  }

  public static boolean isFakeDonorGene(@NonNull ObjectNode donorGene) {
    return isBlank(donorGene, DONOR_GENE_GENE_ID);
  }

  public static Map<String, Object> createFakeGenePOJO() {
    val fakeGene = Maps.<String, Object> newHashMap();
    fakeGene.put(GENE_FAKE_FLAG, Boolean.TRUE);

    return fakeGene;
  }

  public static boolean isFakeGene(Map<String, Object> gene) {
    return gene.containsKey(GENE_FAKE_FLAG) && Boolean.TRUE.equals(gene.get(GENE_FAKE_FLAG));
  }

}
