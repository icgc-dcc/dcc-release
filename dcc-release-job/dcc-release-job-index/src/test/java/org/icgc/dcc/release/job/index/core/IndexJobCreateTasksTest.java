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
package org.icgc.dcc.release.job.index.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.core.document.DocumentType.DONOR_TYPE;
import static org.icgc.dcc.release.core.document.DocumentType.GENE_TYPE;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import lombok.val;

import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.index.config.IndexProperties;
import org.icgc.dcc.release.job.index.task.EsExportTask;
import org.icgc.dcc.release.job.index.task.IndexBigFilesTask;
import org.icgc.dcc.release.job.index.task.IndexTask;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class IndexJobCreateTasksTest {

  private static final String ES_URI = "es://localhost:9300";
  private static final Set<DocumentType> DOCUMENT_TYPES = ImmutableSet.of(DONOR_TYPE, GENE_TYPE);
  private static final int DOCUMENT_TYPE_COUNT = DocumentType.values().length;

  /**
   * Class under test.
   */
  IndexJob job;
  private static final String INDEX_NAME = "icgc21";

  @Test
  public void testCreateTasks_noTasks() throws Exception {
    val properties = new IndexProperties()
        .setEsUri(ES_URI)
        .setExportEsIndex(false)
        .setIndexDocuments(false)
        .setBigDocumentsOnly(false);

    IndexJob job = new IndexJob(properties);
    Collection<Task> tasks = job.createTasks(INDEX_NAME, DOCUMENT_TYPES);
    assertThat(tasks).isEmpty();

    properties.setBigDocumentsOnly(true);
    job = new IndexJob(properties);
    tasks = job.createTasks(INDEX_NAME, DOCUMENT_TYPES);
    assertThat(tasks).isEmpty();
  }

  @Test
  public void testCreateTasks_exportOnly() throws Exception {
    Map<Class<? extends Task>, Integer> expectedTasks = ImmutableMap.of(EsExportTask.class, DOCUMENT_TYPE_COUNT);
    val properties = new IndexProperties()
        .setEsUri(ES_URI)
        .setExportEsIndex(true)
        .setIndexDocuments(false)
        .setBigDocumentsOnly(false);

    IndexJob job = new IndexJob(properties);
    Collection<Task> tasks = job.createTasks(INDEX_NAME, DOCUMENT_TYPES);
    assertThat(tasks).hasSize(DOCUMENT_TYPE_COUNT);
    verifyTasksType(expectedTasks, tasks);

    properties.setBigDocumentsOnly(true);
    job = new IndexJob(properties);
    tasks = job.createTasks(INDEX_NAME, DOCUMENT_TYPES);
    assertThat(tasks).hasSize(DOCUMENT_TYPE_COUNT);
    verifyTasksType(expectedTasks, tasks);
  }

  @Test
  public void testCreateTasks_bigDocsOnly() throws Exception {
    Map<Class<? extends Task>, Integer> expectedTasks = ImmutableMap.of(IndexBigFilesTask.class, 1);
    val properties = new IndexProperties()
        .setEsUri(ES_URI)
        .setExportEsIndex(false)
        .setIndexDocuments(true)
        .setBigDocumentsOnly(true);

    IndexJob job = new IndexJob(properties);
    Collection<Task> tasks = job.createTasks(INDEX_NAME, DOCUMENT_TYPES);
    assertThat(tasks).hasSize(1);
    verifyTasksType(expectedTasks, tasks);
  }

  @Test
  public void testCreateTasks_allIndex() throws Exception {
    Map<Class<? extends Task>, Integer> expectedTasks = ImmutableMap.of(
        IndexBigFilesTask.class, 1,
        IndexTask.class, DOCUMENT_TYPES.size());
    val properties = new IndexProperties()
        .setEsUri(ES_URI)
        .setExportEsIndex(false)
        .setIndexDocuments(true)
        .setBigDocumentsOnly(false);

    IndexJob job = new IndexJob(properties);
    Collection<Task> tasks = job.createTasks(INDEX_NAME, DOCUMENT_TYPES);
    assertThat(tasks).hasSize(DOCUMENT_TYPES.size() + 1);
    verifyTasksType(expectedTasks, tasks);
  }

  @Test
  public void testCreateTasks_allTasks() throws Exception {
    Map<Class<? extends Task>, Integer> expectedTasks = ImmutableMap.of(
        IndexBigFilesTask.class, 1,
        EsExportTask.class, DOCUMENT_TYPE_COUNT,
        IndexTask.class, DOCUMENT_TYPES.size());
    val properties = new IndexProperties()
        .setEsUri(ES_URI)
        .setExportEsIndex(true)
        .setIndexDocuments(true)
        .setBigDocumentsOnly(false);

    IndexJob job = new IndexJob(properties);
    Collection<Task> tasks = job.createTasks(INDEX_NAME, DOCUMENT_TYPES);
    assertThat(tasks).hasSize(DOCUMENT_TYPES.size() + DOCUMENT_TYPE_COUNT + 1);
    verifyTasksType(expectedTasks, tasks);
  }

  @Test
  public void testCreateTasks_exportAndBigDocs() throws Exception {
    Map<Class<? extends Task>, Integer> expectedTasks = ImmutableMap.of(
        IndexBigFilesTask.class, 1,
        EsExportTask.class, DOCUMENT_TYPE_COUNT);
    val properties = new IndexProperties()
        .setEsUri(ES_URI)
        .setExportEsIndex(true)
        .setIndexDocuments(true)
        .setBigDocumentsOnly(true);

    IndexJob job = new IndexJob(properties);
    Collection<Task> tasks = job.createTasks(INDEX_NAME, DOCUMENT_TYPES);
    assertThat(tasks).hasSize(DOCUMENT_TYPE_COUNT + 1);
    verifyTasksType(expectedTasks, tasks);
  }

  private static void verifyTasksType(Map<Class<? extends Task>, Integer> expectedCounts,
      Collection<? extends Task> tasks) {
    for (val entry : expectedCounts.entrySet()) {
      val taskClass = entry.getKey();
      val expectedCount = entry.getValue();
      assertThat(getMatchedCount(tasks, taskClass)).isEqualTo(expectedCount);
    }

  }

  private static int getMatchedCount(Collection<? extends Task> tasks, Class<? extends Task> taskClass) {
    val count = tasks.stream()
        .map(task -> task.getClass())
        .filter(task -> taskClass.equals(task))
        .count();

    return (int) count;
  }

}
