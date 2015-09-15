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
package org.icgc.dcc.release.job.image.util;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Collections.emptySet;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.release.core.util.Stopwatches.createStarted;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Resolves stained images from http://cancer.digitalslidearchive.net given a specimen id.
 */
@Slf4j
public class SpecimenImageResolver {

  /**
   * URLs.
   */
  private static final String BASE_URL = "http://cancer.digitalslidearchive.net";
  private static final String IMAGE_URL = BASE_URL + "/index_mskcc.php";
  private static final String SLIDE_LIST_URL = BASE_URL + "/local_php/get_slide_list_from_db.php";
  private static final String TUMOR_TYPES_URL = BASE_URL + "/local_php/datagroup_combo_connector.php";

  /**
   * URL parameters.
   */
  private static final String TUMOR_TYPE_PARAM_NAME = "tumor_type";
  private static final String SLIDE_NAME_PARAM_NAME = "slide_name";

  /**
   * Constants.
   */
  private static final int URL_TIMEOUT_VALUE = 10 * 1000; // Milliseconds;

  /**
   * Values.
   */
  private static final String NO_MATCH_REPLACEMENT_VALUE = null;

  /**
   * Configuration
   */
  private final boolean validate;

  /**
   * State.
   */
  @Getter
  private final Set<String> availableSpecimenIds;

  /**
   * Creates a resolver with the configured ability to validate if the stained image actually exists.
   */
  public SpecimenImageResolver(boolean validate) {
    this.validate = validate;

    if (!validate) {
      log.warn("**** Not reading available specimen ids. Some resulting URLs may not be valid and thus not available!");
      this.availableSpecimenIds = emptySet();
    } else {
      this.availableSpecimenIds = readAvailableSpecimenIds();
    }
  }

  /**
   * Resolves a specimen image URL given the supplied {@code specimenId}.
   */
  public String resolveUrl(@NonNull String specimenId) {
    if (validate) {
      // Create if value is available
      return availableSpecimenIds.contains(specimenId) ? formatSpecimenImageUrl(specimenId) : NO_MATCH_REPLACEMENT_VALUE;
    } else {
      // Assume available
      return formatSpecimenImageUrl(specimenId);
    }
  }

  public Map<String, String> resolveUrls() {
    checkState(validate, "Cannot resolve urls unless validation is enabled.");

    val builder = ImmutableMap.<String, String> builder();
    for (val specimenId : availableSpecimenIds) {
      builder.put(specimenId, resolveUrl(specimenId));
    }

    val urls = builder.build();
    log.info("Resolved {} urls for {} samples", formatCount(urls.size()), formatCount(availableSpecimenIds.size()));

    return urls;
  }

  @SneakyThrows
  private static Set<String> readAvailableSpecimenIds() {
    val watch = createStarted();
    val tumorTypes = readAvailableTumorTypes();

    // Run queries for each tumor type in parallel
    val service = listeningDecorator(newFixedThreadPool(tumorTypes.size()));
    try {
      val futures = Lists.<ListenableFuture<Set<String>>> newArrayList();

      for (val tumorType : tumorTypes) {
        val tumorTypeUrl = getTumorTypeUrl(tumorType);
        val future = service.submit(new Callable<Set<String>>() {

          @Override
          public Set<String> call() throws Exception {
            return readTumorTypeSpecimenIds(tumorType, tumorTypeUrl);
          }

        });

        futures.add(future);
      }

      // Combine results from each tumor type
      val builder = ImmutableSet.<String> builder();
      for (val tumorTypeIds : allAsList(futures).get()) {
        builder.addAll(tumorTypeIds);
      }

      val availableSpecimenIds = builder.build();
      log.info("Read {} total available specimen ids in {}", formatCount(availableSpecimenIds.size()), watch);

      return availableSpecimenIds;
    } finally {
      service.shutdown();
    }
  }

  private static URL getTumorTypeUrl(String tumorType) throws MalformedURLException {
    return new URL(SLIDE_LIST_URL + "?" + TUMOR_TYPE_PARAM_NAME + "=" + tumorType);
  }

  private static Set<String> readTumorTypeSpecimenIds(String tumorType, URL tumorTypeUrl) throws JDOMException,
      IOException {
    val tumorTypeIds = ImmutableSet.<String> builder();
    val document = readDocument(tumorTypeUrl);
    val root = document.getRootElement();

    val items = root.getChildren("item");
    log.info("Read {} {} tumor type specimen ids", formatCount(items), tumorType);
    for (val item : items) {
      val pyramidFilename = item.getChild("pyramid_filename").getText();

      if (isNullOrEmpty(pyramidFilename)) {
        continue;
      }

      // Chop of the last two parts to get sample/specimen
      val specimenId = parseSpecimenId(pyramidFilename);
      if (isNullOrEmpty(specimenId)) {
        continue;
      }

      tumorTypeIds.add(specimenId);
    }

    return tumorTypeIds.build();
  }

  @SneakyThrows
  private static List<String> readAvailableTumorTypes() {
    val document = readDocument(new URL(TUMOR_TYPES_URL));
    val builder = ImmutableList.<String> builder();

    val root = document.getRootElement();
    val items = root.getChildren("option");
    for (val item : items) {
      val tumorType = item.getAttributeValue("value");
      if (isNullOrEmpty(tumorType)) {
        continue;
      }

      builder.add(tumorType);
    }

    val availableTumorTypes = builder.build();
    log.info("Read available tumor types: {}", availableTumorTypes);
    return availableTumorTypes;
  }

  private static Document readDocument(URL url) throws JDOMException, IOException {
    val connection = url.openConnection();
    connection.setConnectTimeout(URL_TIMEOUT_VALUE);
    connection.setReadTimeout(URL_TIMEOUT_VALUE);
    connection.connect();

    return new SAXBuilder().build(url);
  }

  private static String parseSpecimenId(String specimenSampleId) {
    val sample = specimenSampleId.substring(0, specimenSampleId.lastIndexOf("-"));

    return sample.substring(0, sample.lastIndexOf("-"));
  }

  private static String formatSpecimenImageUrl(String specimenId) {
    return String.format("%s?%s=%s", IMAGE_URL, SLIDE_NAME_PARAM_NAME, specimenId);
  }

}