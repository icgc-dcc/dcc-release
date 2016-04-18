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

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import lombok.NonNull;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.job.export.model.ExportType;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ExportTask extends GenericExportTask {

  public ExportTask(@NonNull String exportDirPath, @NonNull ExportType exportType,
      @NonNull StructType exportTypeSchema, @NonNull SQLContext sqlContext) {
    super(exportDirPath, exportType, exportTypeSchema, sqlContext);
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public String getName() {
    return Task.getName(super.getName(), exportType.getId());
  }

  @Override
  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext) {
    return readUnpartitionedInput(taskContext, exportType.getInputFileType());
  }

  @Override
  protected void writeOutput(TaskContext taskContext, DataFrame output) {
    output
        .write()
        .partitionBy(DONOR_ID)
        .parquet(getOutPath(taskContext));
  }

}
