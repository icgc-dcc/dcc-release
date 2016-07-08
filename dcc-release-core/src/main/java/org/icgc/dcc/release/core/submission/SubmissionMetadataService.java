/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.core.submission;

import static com.google.common.base.Stopwatch.createStarted;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Formats.formatCount;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.meta.Resolver.CodeListsResolver;
import org.icgc.dcc.common.core.meta.Resolver.DictionaryResolver;
import org.icgc.dcc.common.core.model.ValueType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Service for interacting with the DCC submission metada system.
 */
@Slf4j
@Lazy
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class SubmissionMetadataService {

  /**
   * Dependencies.
   */
  @NonNull
  private final DictionaryResolver dictionaryResolver;
  @NonNull
  private final CodeListsResolver codeListsResolver;

  @Getter(value = PRIVATE, lazy = true)
  private final ObjectNode dictionary = resolveDictionary();
  @Getter(value = PRIVATE, lazy = true)
  private final List<ObjectNode> codeLists = resolveCodeLists();

  @Cacheable("metadata")
  public List<SubmissionFileSchema> getMetadata() {
    val watch = createStarted();
    log.info("Getting dictionary...");
    val dictionary = getDictionary();

    log.info("Getting schemas...");
    val schemas = getSchemas(dictionary);

    log.info("Finished getting {} submission schemas in {}", formatCount(schemas), watch);
    return schemas;
  }

  private List<SubmissionFileSchema> getSchemas(ObjectNode dictionary) {
    val schemas = ImmutableList.<SubmissionFileSchema> builder();
    for (val schema : dictionary.get("files")) {
      schemas.add(getSchema(schema));
    }

    return schemas.build();
  }

  private SubmissionFileSchema getSchema(JsonNode schema) {
    val name = schema.get("name").asText();
    val pattern = schema.get("pattern").asText();
    val fields = getFields(schema);

    return new SubmissionFileSchema(name, pattern, fields);
  }

  private List<SubmissionFileField> getFields(JsonNode schema) {
    val fields = ImmutableList.<SubmissionFileField> builder();
    for (val field : schema.withArray("fields")) {
      fields.add(getField(field));
    }

    return fields.build();
  }

  private SubmissionFileField getField(JsonNode field) {
    try {
      val name = field.get("name").asText();
      val type = ValueType.valueOf(field.get("valueType").asText());
      val controlled = field.get("controlled").asBoolean();
      val restrictions = field.get("restrictions");
      val terms = getFieldTerms(restrictions);

      // TODO: Fix in the dictionary!
      val inconsistent = terms != null && type != ValueType.TEXT;
      val effectiveType = inconsistent ? ValueType.TEXT : type;

      return new SubmissionFileField(name, effectiveType, controlled, terms);
    } catch (Exception e) {
      val mesage = "Error getting submission file field from JSON: " + field;
      log.error(mesage, e);
      throw new RuntimeException(mesage, e); // NOPMD
    }
  }

  private Map<String, String> getFieldTerms(JsonNode restrictions) {
    for (val restriction : restrictions) {
      if (isCodeListRestriction(restriction)) {
        val codeListName = restriction.get("config").get("name").asText();
        for (val codeList : getCodeLists()) {
          if (codeList.get("name").asText().equals(codeListName)) {
            val terms = getCodeListTerms(codeList);

            return terms;
          }
        }
      }
    }

    return null;
  }

  private boolean isCodeListRestriction(JsonNode restriction) {
    return restriction.get("type").asText().equals("codelist");
  }

  private Map<String, String> getCodeListTerms(ObjectNode codeList) {
    val terms = Maps.<String, String> newHashMap();
    for (val term : codeList.get("terms")) {
      val code = term.get("code").asText();
      val value = term.get("value").asText();

      terms.put(code, value);
    }

    return terms;
  }

  private ObjectNode resolveDictionary() {
    val watch = createStarted();
    log.info("Resolving dictionary...");
    val dictionary = dictionaryResolver.get();
    log.info("Finished resolving dictionary in {}", watch);

    return dictionary;
  }

  private List<ObjectNode> resolveCodeLists() {
    val watch = createStarted();
    log.info("Resolving code lists...");
    val array = codeListsResolver.get();
    log.info("Finished resolving {} code lists in {}", formatCount(array), watch);

    val codeLists = Lists.<ObjectNode> newArrayList();
    for (val element : array) {
      codeLists.add((ObjectNode) element);
    }

    return codeLists;
  }

}
