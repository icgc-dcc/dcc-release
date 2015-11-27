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
package org.icgc.dcc.release.job.document.util;

import static com.google.common.collect.Iterables.isEmpty;
import static org.icgc.dcc.release.core.util.ObjectNodeFilter.FilterMode.EXCLUDE;
import static org.icgc.dcc.release.core.util.ObjectNodeFilter.FilterMode.INCLUDE;
import lombok.NonNull;
import lombok.ToString;
import lombok.val;

import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.release.core.util.ObjectNodeFilter;
import org.icgc.dcc.release.job.document.model.CollectionFields;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;

@ToString
public class CollectionFieldsFilterAdapter extends ObjectNodeFilter {

  private final boolean mongoConvention;

  public CollectionFieldsFilterAdapter(@NonNull CollectionFields fields) {
    super(getMode(fields), ImmutableSet.copyOf(getPaths(fields)));
    this.mongoConvention = isMongoConvention(fields);
  }

  @Override
  public ObjectNode filter(ObjectNode value) {
    val filtered = super.filter(value);

    // TODO: Remove after no Mongo usages. This is here for migration cleanup purposes.
    if (mongoConvention) {
      filtered.remove(FieldNames.MONGO_INTERNAL_ID);
    }

    return filtered;
  }

  private static Iterable<String> getPaths(CollectionFields fields) {
    return getMode(fields) == INCLUDE ? fields.getIncludedFields() : fields.getExcludedFields();
  }

  private static FilterMode getMode(CollectionFields fields) {
    return !isEmpty(fields.getIncludedFields()) ? INCLUDE : EXCLUDE;
  }

  /**
   * "A projection cannot contain both include and exclude specifications, except for the exclusion of the _id field. In
   * projections that explicitly include fields, the _id field is the only field that you can explicitly exclude."
   * 
   * @see http://docs.mongodb.org/manual/reference/method/db.collection.find/#db.collection.find
   */
  private boolean isMongoConvention(CollectionFields fields) {
    return getMode(fields) == INCLUDE && !isEmpty(fields.getExcludedFields());
  }

}
