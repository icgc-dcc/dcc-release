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
package org.icgc.dcc.release.job.export.task;

import static com.google.common.collect.Iterables.size;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.export.io.RowWriter;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public abstract class AbstractExportTask extends GenericTask {

  /**
   * Dependencies.
   */
  protected final FileType exportType;
  private final Iterable<RowWriter> outputWriters;

  protected AbstractExportTask(
      @NonNull FileType exportType,
      @NonNull Iterable<RowWriter> outputWriters) {
    this.exportType = exportType;
    this.outputWriters = outputWriters;
    log.debug("Created Export task for file type '{}'", exportType);
  }

  @Override
  public void execute(TaskContext taskContext) {
    log.info("Executing export task for '{}'...", exportType.getId());
    // ReadInput
    val input = readInput(taskContext);

    // Save
    writeOutput(taskContext, input);
  }

  protected abstract JavaRDD<ObjectNode> readInput(TaskContext taskContext);

  private void writeOutput(TaskContext taskContext, JavaRDD<ObjectNode> rows) {
    val cache = size(outputWriters) > 1;
    if (cache) {
      rows.cache();
    }

    stream(outputWriters)
        .forEach(writer -> writer.write(taskContext, rows));

    if (cache) {
      rows.unpersist();
    }
  }

}
