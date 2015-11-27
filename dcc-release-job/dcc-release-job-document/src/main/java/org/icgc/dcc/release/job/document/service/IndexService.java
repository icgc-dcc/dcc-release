/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.document.service;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Strings.repeat;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.VersionUtils.getScmInfo;
import static org.icgc.dcc.release.job.document.factory.JacksonFactory.newDefaultMapper;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.Map.Entry;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.icgc.dcc.common.core.model.IndexType;
import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;

@Slf4j
@RequiredArgsConstructor
public class IndexService implements Closeable {

  /**
   * Mapping / index files base path.
   */
  private static final String ES_CONFIG_BASE_PATH = "org/icgc/dcc/etl/resources/mappings";

  /**
   * Dependencies.
   */
  private final Client client;
  @Getter(lazy = true, value = PRIVATE)
  private final IndicesAdminClient indexClient = client.admin().indices();
  @Getter(lazy = true, value = PRIVATE)
  private final ClusterAdminClient clusterClient = client.admin().cluster();

  public void initializeIndex(@NonNull String indexName) {
    val client = getIndexClient();

    log.info("Checking index '{}' for existence...", indexName);
    boolean exists = client.prepareExists(indexName)
        .execute()
        .actionGet()
        .isExists();

    if (exists) {
      log.info("Deleting index '{}'...", indexName);
      checkState(client.prepareDelete(indexName)
          .execute()
          .actionGet()
          .isAcknowledged(),
          "Index '%s' deletion was not acknowledged", indexName);
    }

    try {
      log.info("Creating index '{}'...", indexName);
      checkState(client
          .prepareCreate(indexName)
          .setSettings(IndexService.getSettings().toString())
          .execute()
          .actionGet()
          .isAcknowledged(),
          "Index '%s' creation was not acknowledged!", indexName);

      for (IndexType type : IndexType.values()) {
        String typeName = type.getName();
        String source = IndexService.getTypeMapping(typeName).toString();

        log.info("Creating index '{}' mapping for type '{}'...", indexName, typeName);
        checkState(client.preparePutMapping(indexName)
            .setType(typeName)
            .setSource(source)
            .execute()
            .actionGet()
            .isAcknowledged(),
            "Index '%s' type mapping in index '%s' was not acknowledged for release '%s'!",
            typeName, indexName);
      }
    } catch (Throwable t) {
      propagate(t);
    }
  }

  public void aliasIndex(@NonNull String indexName, @NonNull String alias) {
    val indexNames = getIndexNames();

    try {
      // Remove existing
      val request = getIndexClient().prepareAliases();
      for (val name : indexNames) {
        if (!indexName.equals(name)) {
          request.removeAlias(indexName, alias);
        }
      }

      // Add new
      log.info("Assigning index alias {} to index {}...", alias, indexName);
      request.addAlias(indexName, alias);

      // Re-assign
      checkState(request
          .execute()
          .actionGet()
          .isAcknowledged(),
          "Assigning index alias '%s' to index '%s' was not acknowledged!",
          alias, indexName);
    } catch (Throwable t) {
      propagate(t);
    }
  }

  public void optimizeIndex(@NonNull String indexName) {
    // Optimize the the index for faster search operations by reducing the number of segments by merging
    getIndexClient().prepareOptimize(indexName).execute().actionGet();
  }

  public void freezeIndex(@NonNull String indexName) {
    getIndexClient()
        .prepareUpdateSettings(indexName)
        .setSettings(of("index.blocks.write", (Object) true))
        .execute()
        .actionGet();
  }

  public void reportIndex(@NonNull String indexName) {
    val indicesStats = getIndexClient().prepareStats(indexName).execute().actionGet();
    val indexStats = indicesStats.getIndex(indexName);
    log.info("Index stats for '{}'", indexName);

    for (val indexShardStats : indexStats) {
      log.info("Shard Id: {}", indexShardStats.getShardId());

      for (val shardStats : indexShardStats) {
        log.info("  Docs total count: {} docs", formatCount(shardStats.getStats().getDocs().getCount()));
        log.info("  Docs total size:  {}", formatBytes(shardStats.getStats().getStore().getSizeInBytes()));
      }
    }

    // Total
    log.info(repeat("-", 34));
    log.info("                    {} ", formatBytes(indexStats.getTotal().getStore().getSizeInBytes()));
  }

  public static ObjectNode getSettings() throws IOException {
    String resourceName = format("%s/index.settings.json", ES_CONFIG_BASE_PATH);
    URL settingsFileUrl = getResource(resourceName);

    return (ObjectNode) newDefaultMapper().readTree(settingsFileUrl);
  }

  public static ObjectNode getTypeMapping(String typeName) throws JsonProcessingException, IOException {
    String resourceName = format("%s/%s.mapping.json", ES_CONFIG_BASE_PATH, typeName);
    URL mappingFileUrl = getResource(resourceName);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode typeMapping = mapper.readTree(mappingFileUrl);

    // Add meta data for index-to-indexer traceability
    ObjectNode _meta = (ObjectNode) typeMapping.get(typeName).with("_meta");
    _meta.put("creation_date", DateTime.now().toString());
    for (Entry<String, String> entry : getScmInfo().entrySet()) {
      String key = entry.getKey();
      String value = nullToEmpty(entry.getValue()).replaceAll("\n", " ");
      _meta.put(key, value);
    }

    return (ObjectNode) typeMapping;
  }

  private Set<String> getIndexNames() {
    val state = client.admin()
        .cluster()
        .prepareState()
        .execute()
        .actionGet()
        .getState();

    val result = new ImmutableSet.Builder<String>();
    for (val key : state.getMetaData().aliases().keys()) {
      result.add(key.value);
    }

    return result.build();
  }

  @Override
  public void close() {
    client.close();
  }

}
