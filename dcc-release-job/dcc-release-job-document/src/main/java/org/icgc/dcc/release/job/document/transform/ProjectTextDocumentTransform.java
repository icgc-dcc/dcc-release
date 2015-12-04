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
package org.icgc.dcc.release.job.document.transform;

import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_DISPLAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_PRIMARY_SITE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_TUMOUR_SUBTYPE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_TUMOUR_TYPE;
import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.job.document.context.DefaultDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * {@link DocumentTransform} implementation that creates a project document.
 */
public class ProjectTextDocumentTransform implements DocumentTransform, Function<ObjectNode, Document> {

  private final DocumentContext documentContext;

  public ProjectTextDocumentTransform(DocumentJobContext indexJobContext) {
    this.documentContext = new DefaultDocumentContext(DocumentType.PROJECT_TEXT_TYPE, indexJobContext);
  }

  @Override
  public Document call(ObjectNode project) throws Exception {
    return transformDocument(project, documentContext);
  }

  @Override
  public Document transformDocument(@NonNull ObjectNode project, @NonNull DocumentContext context) {
    // Identifiers
    val projectId = project.get(PROJECT_ID).asText();

    project.set("id", project.remove(PROJECT_ID));
    project.set("primarySite", project.remove(PROJECT_PRIMARY_SITE));
    project.set("name", project.remove(PROJECT_DISPLAY_NAME));
    project.set("tumourType", project.remove(PROJECT_TUMOUR_TYPE));
    project.set("tumourSubtype", project.remove(PROJECT_TUMOUR_SUBTYPE));
    project.put("type", "project");

    return new Document(context.getType(), projectId, project);
  }

}
