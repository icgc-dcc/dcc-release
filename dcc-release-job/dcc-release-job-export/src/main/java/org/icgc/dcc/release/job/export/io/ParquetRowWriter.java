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
package org.icgc.dcc.release.job.export.io;

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.release.core.util.Partitions.getPartitionsCount;

import java.io.File;

import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.export.function.CreateRow;
import org.icgc.dcc.release.job.export.function.KeyByDonor;
import org.icgc.dcc.release.job.export.model.ExportType;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class ParquetRowWriter implements RowWriter {

  /**
   * Dependencies.
   */
  protected final String exportDirPath;
  protected final ExportType exportType;
  protected final StructType exportTypeSchema;
  protected final SQLContext sqlContext;

  @Override
  public final void write(TaskContext taskContext, JavaRDD<ObjectNode> output) {
    val dataFrame = createDataFrame(output, taskContext);
    writeDFOutput(taskContext, dataFrame);
  }

  private DataFrame createDataFrame(JavaRDD<ObjectNode> input, TaskContext taskContext) {
    val partitionsNum = getPartitionsCount(input);

    // Convert to ROW
    val rowRdd = input
        .map(new CreateRow(exportType, exportTypeSchema))
        .groupBy(new KeyByDonor(exportType.getIdPartitions()), partitionsNum * exportType.getParallelismMultiplier())
        .flatMap(tuple -> tuple._2);

    return sqlContext.createDataFrame(rowRdd, exportTypeSchema);
  }

  protected void writeDFOutput(TaskContext taskContext, DataFrame output) {
    output
        .write()
        .partitionBy(DONOR_ID)
        .parquet(getOutPath(taskContext));
  }

  protected String getOutPath(TaskContext taskContext) {
    val exportDir = resolveExportDirPath(taskContext);
    val exportTypeDir = new File(exportDir, exportType.getId());

    return exportTypeDir.getAbsolutePath();
  }

  private File resolveExportDirPath(TaskContext taskContext) {
    val jobContext = taskContext.getJobContext();
    val workingDir = new File(jobContext.getWorkingDir());
    val exportDir = new File(workingDir, exportDirPath);

    return exportDir;
  }

}
