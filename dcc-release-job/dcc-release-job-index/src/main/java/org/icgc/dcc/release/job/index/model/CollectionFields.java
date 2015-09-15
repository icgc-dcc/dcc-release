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
package org.icgc.dcc.release.job.index.model;

import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.MONGO_INTERNAL_ID;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import com.google.common.collect.ImmutableList;

/**
 * Metadata for describing which collection fields to include and exclude.
 * <p>
 * A field field cannot contain both include and exclude specifications, except for the exclusion of the _id field. In
 * projections that explicitly include fields, the _id field is the only field that you can explicitly exclude.
 */
@RequiredArgsConstructor(access = PRIVATE)
@Getter
public class CollectionFields {

  /**
   * Include all fields, exclude {@code _id}
   */
  public static final CollectionFields DEFAULT_COLLECTION_FIELDS = collectionFields().build();

  /**
   * The names of fields to include.
   */
  @NonNull
  private final Iterable<String> includedFields;

  /**
   * The names of fields to exclude.
   */
  @NonNull
  private final Iterable<String> excludedFields;

  public static Builder collectionFields() {
    return new Builder();
  }

  public boolean uses(String fieldName) {
    val excluded = contains(excludedFields, fieldName);
    val included = contains(includedFields, fieldName);

    if (isExcludedEmpty()) {
      return included;
    } else {
      return excluded ? false : included;
    }
  }

  private boolean isExcludedEmpty() {
    return isEmpty(excludedFields) || size(excludedFields) == 1 && get(excludedFields, 0).equals(MONGO_INTERNAL_ID);
  }

  public CollectionFields with(String fieldName) {
    val result = collectionFields();

    // Exclude
    if (contains(excludedFields, fieldName)) {
      val newExcludedFields = newArrayList(excludedFields);
      newExcludedFields.remove(fieldName);

      result.excludedFields(newExcludedFields);
    }

    // Include
    if (!isEmpty(includedFields)) {
      val newIncludedFields = newArrayList(includedFields);
      newIncludedFields.add(fieldName);

      result.includedFields(newIncludedFields);
    }

    return result.build();
  }

  public static class Builder {

    /**
     * Constants.
     */
    private static final Iterable<String> DEFAULT_INCLUDE_FIELDS = emptyList();
    private static final Iterable<String> DEFAULT_EXCLUDE_FIELDS = immutableCopy(MONGO_INTERNAL_ID);

    /**
     * State.
     */
    private Iterable<String> includedFields = DEFAULT_INCLUDE_FIELDS;
    private Iterable<String> excludedFields = DEFAULT_EXCLUDE_FIELDS;

    private Builder() {
    }

    public Builder includedFields(String... fieldNames) {
      this.includedFields = immutableCopy(fieldNames);
      return this;
    }

    public Builder includedFields(Iterable<String> fieldNames) {
      this.includedFields = immutableCopy(fieldNames);
      return this;
    }

    public Builder excludedFields(String... fieldNames) {
      this.excludedFields = immutableCopy(fieldNames);
      return this;
    }

    public Builder excludedFields(Iterable<String> fieldNames) {
      this.excludedFields = immutableCopy(fieldNames);
      return this;
    }

    private static ImmutableList<String> immutableCopy(String... fieldNames) {
      return ImmutableList.copyOf(fieldNames);
    }

    private static ImmutableList<String> immutableCopy(Iterable<String> fieldNames) {
      return ImmutableList.copyOf(fieldNames);
    }

    public CollectionFields build() {
      return new CollectionFields(includedFields, excludedFields);
    }

  }

}
