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

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN_ID;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;
import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.job.document.context.DefaultDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * {@link DocumentTransform} implementation that creates a donor document.
 */
public class DonorTextDocumentTransform implements DocumentTransform, Function<ObjectNode, Document> {

  private final DocumentContext documentContext;

  public DonorTextDocumentTransform(DocumentJobContext indexJobContext) {
    this.documentContext = new DefaultDocumentContext(DocumentType.DONOR_TEXT_TYPE, indexJobContext);
  }

  @Override
  public Document call(ObjectNode donor) throws Exception {
    return transformDocument(donor, documentContext);
  }

  @Override
  public Document transformDocument(@NonNull ObjectNode donor, @NonNull DocumentContext context) {
    // Identifiers
    val donorId = getDonorId(donor);
    val specimen = donor.path(DONOR_SPECIMEN);

    val specimenIds = Sets.<String> newLinkedHashSet();
    val submittedSpecimenIds = Sets.<String> newLinkedHashSet();

    val samples = Lists.<JsonNode> newArrayList();
    val sampleIds = Sets.<String> newLinkedHashSet();
    val submittedSampleIds = Sets.<String> newLinkedHashSet();

    if (specimen.isArray()) {
      for (JsonNode node : specimen) {
        specimenIds.add(node.get(DONOR_SPECIMEN_ID).asText());
        submittedSpecimenIds.add(node.get("specimen_id").asText());
        val sampleList = node.get(DONOR_SAMPLE);
        if (sampleList.isArray()) {
          for (JsonNode sample : sampleList) {
            samples.add(sample);
          }
        }
      }

      for (JsonNode node : samples) {
        sampleIds.add(node.get(DONOR_SAMPLE_ID).asText());
        submittedSampleIds.add(node.get(DONOR_SAMPLE_ANALYZED_SAMPLE_ID).asText());
      }
    }

    donor.put("id", donorId);
    donor.set("projectId", donor.get(DONOR_PROJECT_ID));
    donor.set("submittedId", donor.get("donor_id"));

    donor.putPOJO("specimenIds", specimenIds);
    donor.putPOJO("submittedSpecimenIds", submittedSpecimenIds);
    donor.putPOJO("sampleIds", sampleIds);
    donor.putPOJO("submittedSampleIds", submittedSampleIds);

    donor.put("type", "donor");

    donor.remove(DONOR_ID);
    donor.remove(DONOR_PROJECT_ID);
    donor.remove("donor_id");
    donor.remove(DONOR_SPECIMEN);
    return new Document(context.getType(), donorId, donor);
  }
}
