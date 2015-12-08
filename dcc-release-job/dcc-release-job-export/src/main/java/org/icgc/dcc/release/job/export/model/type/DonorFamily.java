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
package org.icgc.dcc.release.job.export.model.type;

import static org.icgc.dcc.release.job.export.model.type.Constants.FAMILY_FIELD_NAME;

import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.function.FlattenField;
import org.icgc.dcc.release.core.function.ParseObjectNode;
import org.icgc.dcc.release.core.function.ProjectFields;
import org.icgc.dcc.release.core.function.PullUpField;
import org.icgc.dcc.release.core.function.RetainFields;
import org.icgc.dcc.release.job.export.function.AddDonorIdField;
import org.icgc.dcc.release.job.export.function.IsNonEmpty;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class DonorFamily implements Type {

  private static final String DATA_TYPE_FOLDER = "donor";

  private static final ImmutableMap<String, String> FIRST_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("donor_id", "submitted_donor_id")
      .put(FAMILY_FIELD_NAME, FAMILY_FIELD_NAME)
      .build();

  private static final ImmutableMap<String, String> SECOND_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("donor_id", "donor_id")
      .put("donor_has_relative_with_cancer_history", "donor_has_relative_with_cancer_history")
      .put("relationship_type", "relationship_type")
      .put("relationship_type_other", "relationship_type_other")
      .put("relationship_sex", "relationship_sex")
      .put("relationship_age", "relationship_age")
      .put("relationship_disease_icd10", "relationship_disease_icd10")
      .put("relationship_disease", "relationship_disease")
      .build();

  @Override
  public JavaRDD<ObjectNode> process(JavaRDD<String> input) {
    return input
        .map(new ParseObjectNode())
        .map(new ProjectFields(FIRST_LEVEL_PROJECTION))
        .map(new AddDonorIdField())
        .filter(new IsNonEmpty(FAMILY_FIELD_NAME))
        .flatMap(new FlattenField(FAMILY_FIELD_NAME))
        .map(new PullUpField(FAMILY_FIELD_NAME))
        .map(new RetainFields(getFields()));
  }

  @Override
  public Set<String> getFields() {
    return Sets.newHashSet(Iterables.concat(FIRST_LEVEL_PROJECTION.values(), SECOND_LEVEL_PROJECTION.keySet()));
  }

  @Override
  public String getTypeDirectoryName() {
    return DATA_TYPE_FOLDER;
  }
}
