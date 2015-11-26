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
package org.icgc.dcc.release.core.util;

import static com.google.common.collect.ImmutableList.of;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.CNGV_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.STGV_TYPE;
import static org.icgc.dcc.release.core.util.ObjectNodes.createBooleanNode;
import static org.icgc.dcc.release.core.util.ObjectNodes.createNumberNode;

import java.util.List;

import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

@NoArgsConstructor(access = PRIVATE)
public final class FeatureTypes {

  // TODO: Types are not present in the dictionary and not displayed on the portal. Must be removed from the ETL 1.
  private static final List<FeatureType> SKIP_TYPE = of(CNGV_TYPE, STGV_TYPE);

  public static Iterable<FeatureType> getFeatureTypes() {
    val featureTypes = Lists.newArrayList(FeatureType.values());
    featureTypes.removeAll(SKIP_TYPE);

    return featureTypes;
  }

  public static JsonNode createFeatureTypeSummaryValue(FeatureType featureType, int featureTypeCount) {
    val hasFeatureType = featureTypeCount > 0;

    return featureType.isCountSummary() ? createNumberNode(featureTypeCount) : createBooleanNode(hasFeatureType);
  }

}
