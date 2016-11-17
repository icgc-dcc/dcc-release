/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.index.service;

import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.release.core.document.DocumentType.DONOR_CENTRIC_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.DONOR_TEXT_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.DONOR_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.DRUG_CENTRIC_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.DRUG_TEXT_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.GENE_CENTRIC_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.GENE_SET_TEXT_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.GENE_SET_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.GENE_TEXT_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.GENE_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.MUTATION_CENTRIC_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.MUTATION_TEXT_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.PROJECT_TEXT_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.PROJECT_TYPE;

import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.client.Client;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.release.core.document.DocumentType;

import com.google.common.collect.ImmutableSet;

@Slf4j
@RequiredArgsConstructor
public class IndexVerificationService {

  @NonNull
  private final Client client;
  @NonNull
  private final String indexName;

  public void verify() {
    verifyCounts();
  }

  private void verifyCounts() {
    verifyGroupCounts(ImmutableSet.of(DONOR_TYPE, DONOR_TEXT_TYPE, DONOR_CENTRIC_TYPE));
    verifyGroupCounts(ImmutableSet.of(GENE_TYPE, GENE_TEXT_TYPE, GENE_CENTRIC_TYPE));
    verifyGroupCounts(ImmutableSet.of(GENE_SET_TYPE, GENE_SET_TEXT_TYPE));
    verifyGroupCounts(ImmutableSet.of(DRUG_TEXT_TYPE, DRUG_CENTRIC_TYPE));
    verifyGroupCounts(ImmutableSet.of(MUTATION_CENTRIC_TYPE, MUTATION_TEXT_TYPE));
    verifyGroupCounts(ImmutableSet.of(PROJECT_TYPE, PROJECT_TEXT_TYPE));
  }

  private void verifyGroupCounts(Set<DocumentType> typesGroup) {
    val groupCounts = typesGroup.stream()
        .collect(toImmutableMap(type -> type, this::getTypeCount));

    val sampleCount = groupCounts.values().iterator().next();
    val allEqual = groupCounts.values().stream()
        .allMatch(count -> count.equals(sampleCount));

    if (allEqual) {
      val types = getTypes(typesGroup);
      log.info("{} count: {}", types, sampleCount);
    } else {
      log.error("Incorrect documents count for type group detected: {}", getFormattedOutput(groupCounts));
      throw new AssertionError("Document counts mismatch");
    }
  }

  private long getTypeCount(DocumentType type) {
    return client.prepareSearch(indexName)
        .setTypes(type.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
  }

  private static String getTypes(Set<DocumentType> types) {
    val typeNames = types.stream()
        .map(DocumentType::getName)
        .collect(toImmutableList());

    return Joiners.COMMA.join(typeNames);
  }

  private static String getFormattedOutput(Map<DocumentType, Long> groupCounts) {
    val typeCounts = groupCounts.entrySet().stream()
        .map(e -> format("%s: %s", e.getKey().getName(), e.getValue()))
        .collect(toImmutableList());

    return Joiners.COMMA.join(typeCounts);
  }

}
