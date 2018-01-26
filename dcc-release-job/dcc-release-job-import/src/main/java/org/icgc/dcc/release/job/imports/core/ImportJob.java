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
package org.icgc.dcc.release.job.imports.core;

import static org.icgc.dcc.common.core.model.ReleaseCollection.DIAGRAM_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DRUG_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_SET_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.CLINVAR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.CIVIC_COLLECTION;
import static org.icgc.dcc.release.core.job.FileType.DIAGRAM;
import static org.icgc.dcc.release.core.job.FileType.DRUG;
import static org.icgc.dcc.release.core.job.FileType.GENE;
import static org.icgc.dcc.release.core.job.FileType.GENE_SET;
import static org.icgc.dcc.release.core.job.FileType.PROJECT;
import static org.icgc.dcc.release.core.job.FileType.CLINVAR;
import static org.icgc.dcc.release.core.job.FileType.CIVIC;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.job.imports.config.MongoProperties;
import org.icgc.dcc.release.job.imports.task.MongoImportTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class ImportJob extends GenericJob {

  /**
   * Constants.
   */
  private static final String MONGO_REFERENCE_DB = "dcc-import";

  /**
   * Dependencies.
   */
  @NonNull
  private final MongoProperties properties;

  @Override
  public JobType getType() {
    return JobType.IMPORT;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    imports(jobContext);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext, PROJECT, GENE, GENE_SET, DIAGRAM, DRUG, CLINVAR, CIVIC);
  }

  private void imports(JobContext jobContext) {
    jobContext.execute(
        new MongoImportTask(properties, MONGO_REFERENCE_DB, PROJECT_COLLECTION.getId(), PROJECT),
        new MongoImportTask(properties, MONGO_REFERENCE_DB, GENE_COLLECTION.getId(), GENE),
        new MongoImportTask(properties, MONGO_REFERENCE_DB, GENE_SET_COLLECTION.getId(), GENE_SET),
        new MongoImportTask(properties, MONGO_REFERENCE_DB, DRUG_COLLECTION.getId(), DRUG),
        new MongoImportTask(properties, MONGO_REFERENCE_DB, DIAGRAM_COLLECTION.getId(), DIAGRAM),
        new MongoImportTask(properties, MONGO_REFERENCE_DB, CLINVAR_COLLECTION.getId(), CLINVAR),
        new MongoImportTask(properties, MONGO_REFERENCE_DB, CIVIC_COLLECTION.getId(), CIVIC));
  }
}
