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
package org.icgc.dcc.etl2.job.index.util;

import static com.google.common.collect.Iterables.isEmpty;
import static org.icgc.dcc.etl2.core.util.ObjectNodeFilter.FilterMode.EXCLUDE;
import static org.icgc.dcc.etl2.core.util.ObjectNodeFilter.FilterMode.INCLUDE;
import lombok.NonNull;
import lombok.ToString;

import org.icgc.dcc.etl2.core.util.ObjectNodeFilter;
import org.icgc.dcc.etl2.job.index.model.CollectionFields;

@ToString
public class CollectionFieldsFilterAdapter extends ObjectNodeFilter {

  public CollectionFieldsFilterAdapter(@NonNull CollectionFields fields) {
    super(getMode(fields), getPaths(fields));
  }

  private static Iterable<String> getPaths(CollectionFields fields) {
    return getMode(fields) == EXCLUDE ? fields.getExcludedFields() : fields.getIncludedFields();
  }

  private static FilterMode getMode(CollectionFields fields) {
    return !isEmpty(fields.getExcludedFields()) ? EXCLUDE : INCLUDE;
  }

}
