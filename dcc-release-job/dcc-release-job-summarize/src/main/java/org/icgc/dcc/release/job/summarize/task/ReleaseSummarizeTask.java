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
package org.icgc.dcc.release.job.summarize.task;

import static com.google.common.collect.Iterables.getFirst;
import static java.util.Collections.singletonList;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.MONGO_INTERNAL_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_PRIMARY_SITE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_STATE;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_DATE;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_LIVE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_LIVE_PRIMARY_SITE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_LIVE_PROJECT_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_MUTATED_GENE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_NUMBER;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_PRIMARY_SITE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_PROJECT_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SPECIMEN_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SSM_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SPECIMEN_COUNT;
import static org.icgc.dcc.common.core.util.Splitters.DASH;
import static org.icgc.dcc.release.core.job.FileType.GENE_SUMMARY;
import static org.icgc.dcc.release.core.job.FileType.MUTATION;
import static org.icgc.dcc.release.core.util.ObjectNodes.createObject;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.function.RetainFields;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.Ints;

@RequiredArgsConstructor
public class ReleaseSummarizeTask extends GenericTask {

  private final long donorsCount;
  private final long liveDonorsCount;

  @Override
  public void execute(TaskContext taskContext) {
    val release = createReleaseSummary(taskContext);
    val output = taskContext.getSparkContext().parallelize(singletonList(release));
    writeOutput(taskContext, output, FileType.RELEASE_SUMMARY);
  }

  private ObjectNode createReleaseSummary(TaskContext taskContext) {
    val release = createObject();
    val releaseId = getReleaseId(taskContext);
    val releaseName = resolveReleaseName(releaseId);

    // Release metadata
    release.put(MONGO_INTERNAL_ID, releaseId);
    release.put(RELEASE_ID, releaseId);
    release.put(RELEASE_NAME, releaseName);
    release.put(RELEASE_NUMBER, parseReleaseNumber(releaseName));
    release.put(RELEASE_DATE, DateTime.now().toString());

    // Project counts
    val projects = readProjects(taskContext);
    val projectsCount = projects.count();
    val liveProjectCount = getLiveProjectCount(projects);
    val uniqueLivePrimarySiteCount = getUniqueLivePrimarySiteCount(projects);
    val releaseSpecimenCount = getReleaseSpecimenCount(projects);
    val releaseSampleCount = getReleaseSampleCount(projects);
    val uniquePrimarySiteCount = getUniquePrimarySiteCount(projects);

    release.put(RELEASE_PROJECT_COUNT, projectsCount);
    release.put(RELEASE_LIVE_PROJECT_COUNT, liveProjectCount);
    release.put(RELEASE_PRIMARY_SITE_COUNT, uniquePrimarySiteCount);
    release.put(RELEASE_LIVE_PRIMARY_SITE_COUNT, uniqueLivePrimarySiteCount);

    // Donor counts
    release.put(RELEASE_DONOR_COUNT, donorsCount);
    release.put(RELEASE_LIVE_DONOR_COUNT, liveDonorsCount);
    release.put(RELEASE_SPECIMEN_COUNT, releaseSpecimenCount);
    release.put(RELEASE_SAMPLE_COUNT, releaseSampleCount);

    // Observation counts
    release.put(RELEASE_SSM_COUNT, getUniqueSsmCount(taskContext));
    release.put(RELEASE_MUTATED_GENE_COUNT, getUniqueAffectedGeneCount(taskContext));

    return release;
  }

  private static long getReleaseSampleCount(JavaRDD<ObjectNode> projects) {
    return projects
        .map(o -> o.get(PROJECT_SUMMARY).get(TOTAL_SAMPLE_COUNT).asLong())
        .reduce((a, b) -> a + b);
  }

  private static long getReleaseSpecimenCount(JavaRDD<ObjectNode> projects) {
    return projects
        .map(o -> o.get(PROJECT_SUMMARY).get(TOTAL_SPECIMEN_COUNT).asLong())
        .reduce((a, b) -> a + b);
  }

  private long getUniqueAffectedGeneCount(TaskContext taskContext) {
    return readInput(taskContext, GENE_SUMMARY)
        .filter(o -> !o.path(GENE_PROJECTS).isMissingNode())
        .count();
  }

  private long getUniqueSsmCount(TaskContext taskContext) {
    return readInput(taskContext, MUTATION)
        .count();
  }

  private static long getUniqueLivePrimarySiteCount(JavaRDD<ObjectNode> projects) {
    return projects
        .filter(o -> isLiveProject(o))
        .map(new RetainFields(PROJECT_PRIMARY_SITE))
        .distinct()
        .count();
  }

  private static long getUniquePrimarySiteCount(JavaRDD<ObjectNode> projects) {
    return projects
        .map(new RetainFields(PROJECT_PRIMARY_SITE))
        .distinct()
        .count();
  }

  private static long getLiveProjectCount(JavaRDD<ObjectNode> projects) {
    return projects
        .filter(o -> isLiveProject(o))
        .count();
  }

  private static boolean isLiveProject(ObjectNode row) {
    return textValue(row.get(PROJECT_SUMMARY), PROJECT_SUMMARY_STATE).equals("live");
  }

  private static Integer parseReleaseNumber(String releaseName) {
    // TODO: Pass explicitly from CLI
    return Ints.tryParse(releaseName.replaceAll("\\D+", ""));
  }

  private static String resolveReleaseName(String releaseId) {
    return getFirst(DASH.split(releaseId), "");
  }

  private static String getReleaseId(TaskContext taskContext) {
    return taskContext.getJobContext().getReleaseName();
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  private JavaRDD<ObjectNode> readProjects(TaskContext taskContext) {
    return readInput(taskContext, FileType.PROJECT_SUMMARY);
  }

}
