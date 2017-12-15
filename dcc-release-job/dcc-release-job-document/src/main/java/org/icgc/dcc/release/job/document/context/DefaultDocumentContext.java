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
package org.icgc.dcc.release.job.document.context;

import static java.lang.String.format;
import static org.icgc.dcc.release.job.document.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.release.job.document.util.Fakes.createFakeGene;

import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class DefaultDocumentContext implements DocumentContext {

  @NonNull
  private final DocumentType type;
  @NonNull
  private final DocumentJobContext documentJobContext;

  @Getter(lazy = true)
  private final Map<String, ObjectNode> projects = filterProjects();
  @Getter(lazy = true)
  private final Map<String, ObjectNode> genes = filterGenes();
  @Getter(lazy = true)
  private final Map<String, ObjectNode> donors = filterDonors();
  @Getter(lazy = true)
  private final Map<String, ObjectNode> clinvar = loadClinvar();
  @Getter(lazy = true)
  private final Map<String, Iterable<ObjectNode>> civic = loadCivic();

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

  public ObjectNode getClinvar(String annotationId) {
    return getClinvar().get(annotationId);
  }

  public Iterable<ObjectNode> getCivic(String annotationId) {
    return getCivic().get(annotationId);
  }

  @Override
  public Iterable<ObjectNode> getGenesByGeneSetId(String geneSetId) {
    throw throwUnsupportedOperationException();
  }

  @Override
  public Iterable<ObjectNode> getObservationsByDonorId(String donorId) {
    throw throwUnsupportedOperationException();
  }

  @Override
  public Iterable<ObjectNode> getObservationsByGeneId(String geneId) {
    throw throwUnsupportedOperationException();
  }

  @Override
  public Iterable<ObjectNode> getObservationsByMutationId(String mutationId) {
    throw throwUnsupportedOperationException();
  }

  private Map<String, ObjectNode> filterGenes() {
    return documentJobContext.getGenesBroadcast().getValue();
  }

  private Map<String, ObjectNode> filterProjects() {
    return documentJobContext.getProjectsBroadcast().getValue();
  }

  private Map<String, ObjectNode> filterDonors() {
    return documentJobContext.getDonorsBroadcast().getValue();
  }

  private Map<String, ObjectNode> loadClinvar() {
    return documentJobContext.getClinvarBroadcast().getValue();
  }

  private Map<String, Iterable<ObjectNode>> loadCivic() {
    return documentJobContext.getCivicBroadcast().getValue();
  }

  private UnsupportedOperationException throwUnsupportedOperationException() {
    return new UnsupportedOperationException(format("This method should not be called for %s", this.getClass()
        .getName()));
  }

}
