package org.icgc.dcc.etl2.core.submission;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.ValueType;
import org.icgc.dcc.common.core.util.resolver.RestfulCodeListsResolver;
import org.icgc.dcc.common.core.util.resolver.RestfulDictionaryResolver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Slf4j
@Service
@Setter
public class SubmissionMetadataRepository {

  /**
   * Configuration.
   */
  @Value("${dcc.submission.url}")
  private String submissionUrl;

  @Getter(value = PRIVATE, lazy = true)
  private final ObjectNode dictionary = resolveDictionary();
  @Getter(value = PRIVATE, lazy = true)
  private final List<ObjectNode> codeLists = resolveCodeLists();

  @Cacheable("schemas")
  public List<Schema> getSchemas() {
    val watch = createStarted();
    log.info("Getting dictionary...");
    val dictionary = getDictionary();

    val schemas = ImmutableList.<Schema> builder();
    for (val schema : dictionary.get("files")) {
      schemas.add(getSchema(schema));
    }

    log.info("Finished getting metadata in {}", watch);
    return schemas.build();
  }

  private Schema getSchema(JsonNode schema) {
    val name = schema.get("name").asText();
    val pattern = schema.get("pattern").asText();
    val fields = getFields(schema);

    log.info("Schema '{}' ({}): {}", new Object[] { name, pattern, fields });
    return new Schema(name, pattern, fields);
  }

  private List<Field> getFields(JsonNode schema) {
    val fields = ImmutableList.<Field> builder();
    for (val field : schema.withArray("fields")) {
      fields.add(getField(field));
    }

    return fields.build();
  }

  private Field getField(JsonNode field) {
    val name = field.get("name").asText();
    val type = ValueType.valueOf(field.get("valueType").asText());
    val restrictions = field.get("restrictions");
    val terms = getFieldTerms(restrictions);

    // TODO: Fix in the dictionary!
    val inconsistent = terms != null && type != ValueType.TEXT;
    val effectiveType = inconsistent ? ValueType.TEXT : type;

    return new Field(name, effectiveType, terms);
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
    log.info("Resolving dictionary from '{}'...", submissionUrl);
    val resolver = new RestfulDictionaryResolver(submissionUrl);
    val dictionary = resolver.get();
    log.info("Finished resolving dictionary in {}", watch);

    return dictionary;
  }

  private List<ObjectNode> resolveCodeLists() {
    val watch = createStarted();
    log.info("Resolving code lists from '{}'...", submissionUrl);
    val resolver = new RestfulCodeListsResolver(submissionUrl);
    val array = resolver.get();
    log.info("Finished resolving code lists in {}", watch);

    val codeLists = Lists.<ObjectNode> newArrayList();
    for (val element : array) {
      codeLists.add((ObjectNode) element);
    }

    return codeLists;
  }

}
