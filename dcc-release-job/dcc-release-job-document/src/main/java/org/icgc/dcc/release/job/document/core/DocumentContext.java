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
package org.icgc.dcc.release.job.document.core;

import java.io.Serializable;
import java.util.Map;

import org.icgc.dcc.release.job.document.model.DocumentType;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Reference resources for document construction.
 */
public interface DocumentContext extends Serializable {

  /**
   * Gets the document type under construction.
   * 
   * @return the type
   */
  DocumentType getType();

  /**
   * Gets a complete mapping from {@code _project_id} to project objects.
   * 
   * @return the mapping
   */
  Map<String, ObjectNode> getProjects();

  /**
   * Gets a complete mapping from {@code _donor_id} to donor objects.
   * 
   * @return the mapping
   */
  Map<String, ObjectNode> getDonors();

  /**
   * Gets a complete mapping from {@code _gene_id} to gene objects.
   * 
   * @return the mapping
   */
  Map<String, ObjectNode> getGenes();

  /**
   * Gets a project by {@code _project_id}.
   * 
   * @param projectId the {@code _project_id} of the project
   * @return the project
   */
  ObjectNode getProject(String projectId);

  /**
   * Gets a project by {@code _donor_id}.
   * 
   * @param donorId the {@code _donor_id} of the donor
   * @return the donor
   */
  ObjectNode getDonor(String donorId);

  /**
   * Gets a gene by {@code _gene_id}.
   * 
   * @param geneId the {@code _gene_id} of the gene
   * @return the gene
   */
  ObjectNode getGene(String geneId);

  /**
   * Gets the complete strem of genes by {@code geneSet.id}.
   * 
   * @param geneSetId the {@code  geneSet.id}
   * @return the gene
   */
  Iterable<ObjectNode> getGenesByGeneSetId(String geneSetId);

  /**
   * Gets the complete stream of observations associated with the supplied {@code donorId}.
   * 
   * @param donorId the {@code _donor_id} of the donor
   * @return the donor's observations
   */
  Iterable<ObjectNode> getObservationsByDonorId(String donorId);

  /**
   * Gets the complete stream of observations associated with the supplied {@code geneId}.
   * 
   * @param geneId the {@code _gene_id} of the consequence
   * @return the gene's observations
   */
  Iterable<ObjectNode> getObservationsByGeneId(String geneId);

  /**
   * Gets the complete stream of observations associated with the supplied {@code mutationId}.
   * 
   * @param mutationId the {@code _mutation_id} of the mutation
   * @return the mutation's observations
   */
  Iterable<ObjectNode> getObservationsByMutationId(String mutationId);

}
