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

import java.io.Closeable;
import java.io.IOException;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.release.job.document.model.CollectionFields;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Abstract document source collection reader contract.
 */
public interface CollectionReader extends Closeable {

  Iterable<ObjectNode> readReleases(CollectionFields fields);

  Iterable<ObjectNode> readProjects(CollectionFields fields);

  Iterable<ObjectNode> readDonors(CollectionFields fields);

  Iterable<ObjectNode> readGenes(CollectionFields fields);

  Iterable<ObjectNode> readGenesPivoted(CollectionFields fields);

  Iterable<ObjectNode> readGeneSets(CollectionFields fields);

  Iterable<ObjectNode> readObservations(CollectionFields fields);

  Iterable<ObjectNode> readObservationsByDonorId(String donorId, CollectionFields fields);

  Iterable<ObjectNode> readObservationsByGeneId(String geneId, CollectionFields fields);

  Iterable<ObjectNode> readObservationsByMutationId(String mutationId, CollectionFields observationFields);

  Iterable<ObjectNode> readMutations(CollectionFields fields);

  Iterable<ObjectNode> read(ReleaseCollection collection, CollectionFields fields);

  @Override
  void close() throws IOException;

}