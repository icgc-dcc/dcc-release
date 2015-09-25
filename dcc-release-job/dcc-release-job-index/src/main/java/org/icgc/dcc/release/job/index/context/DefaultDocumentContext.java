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
package org.icgc.dcc.release.job.index.context;

import static java.lang.String.format;
import static org.icgc.dcc.release.job.index.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.release.job.index.util.Fakes.createFakeGene;

import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.release.core.function.FilterFields;
import org.icgc.dcc.release.job.index.core.DocumentContext;
import org.icgc.dcc.release.job.index.core.IndexJobContext;
import org.icgc.dcc.release.job.index.model.CollectionFields;
import org.icgc.dcc.release.job.index.model.DocumentType;
import org.icgc.dcc.release.job.index.util.CollectionFieldsFilterAdapter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

@RequiredArgsConstructor
public class DefaultDocumentContext implements DocumentContext {

  @NonNull
  private final DocumentType type;
  @NonNull
  private final IndexJobContext indexJobContext;

  @Getter(lazy = true)
  private final Map<String, ObjectNode> projects = filterProjects();
  @Getter(lazy = true)
  private final Map<String, ObjectNode> genes = filterGenes();
  @Getter(lazy = true)
  private final Map<String, ObjectNode> donors = filterDonors();

  @Override
  public DocumentType getType() {
    return type;
  }

  @Override
  public ObjectNode getProject(String projectId) {
    return getProjects().get(projectId);
  }

  @Override
  public ObjectNode getDonor(String donorId) {
    return getDonors().get(donorId);
  }

  @Override
  public ObjectNode getGene(String geneId) {
    if (geneId.equals(FAKE_GENE_ID)) {
      return createFakeGene();
    }

    return getGenes().get(geneId);
  }

  @Override
  public Iterable<ObjectNode> getGenesByGeneSetId(String geneSetId) {
    throw new UnsupportedOperationException(format("This method should not be called for ", this.getClass().getName()));
  }

  @Override
  public Iterable<ObjectNode> getObservationsByDonorId(String donorId) {
    throw new UnsupportedOperationException(format("This method should not be called for ", this.getClass().getName()));
  }

  @Override
  public Iterable<ObjectNode> getObservationsByGeneId(String geneId) {
    throw new UnsupportedOperationException(format("This method should not be called for ", this.getClass().getName()));
  }

  @Override
  public Iterable<ObjectNode> getObservationsByMutationId(String mutationId) {
    throw new UnsupportedOperationException(format("This method should not be called for ", this.getClass().getName()));
  }

  private Map<String, ObjectNode> filterGenes() {
    val fields = type.getFields().getGeneFields();

    return filterFields(indexJobContext.getGenesBroadcast().getValue(), fields);
  }

  private Map<String, ObjectNode> filterProjects() {
    val fields = type.getFields().getProjectFields();

    return filterFields(indexJobContext.getProjectsBroadcast().getValue(), fields);
  }

  private Map<String, ObjectNode> filterDonors() {
    val fields = type.getFields().getDonorFields();

    return filterFields(indexJobContext.getDonorsBroadcast().getValue(), fields);
  }

  private static Map<String, ObjectNode> filterFields(Map<String, ObjectNode> sourceCollection, CollectionFields fields) {
    val filterFieldsFunction = new FilterFields(new CollectionFieldsFilterAdapter(fields));
    val filteredCollection = ImmutableMap.<String, ObjectNode> builder();
    for (val entry : sourceCollection.entrySet()) {
      filteredCollection.put(entry.getKey(), filterFieldsFunction.call(entry.getValue().deepCopy()));
    }

    return filteredCollection.build();
  }

}
