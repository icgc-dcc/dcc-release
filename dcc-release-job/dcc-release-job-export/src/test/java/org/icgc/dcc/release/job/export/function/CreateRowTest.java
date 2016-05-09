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
package org.icgc.dcc.release.job.export.function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.test.util.TestJsonNodes.$;

import java.util.List;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.release.job.export.model.ExportType;
import org.icgc.dcc.release.job.export.stats.StatsCalculator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.clearspring.analytics.util.Lists;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class CreateRowTest {

  @Mock
  StatsCalculator statsCalculator;

  CreateRow function;

  @Test
  public void callTest() {
    function = new CreateRow(ExportType.SAMPLE, createStructType(), statsCalculator);
    val sourceNode = $("{_project_id:'BRCA-UK',_sample_id:'SA000002',available_raw_sequence_data:["
        + "{raw_data_accession:'123',repository:'EGA',library_strategy:'WGS'}]}");

    val row = function.call(sourceNode);
    log.debug("Row: {}", row);
    assertThat(row.length()).isEqualTo(4);
    @SuppressWarnings("unchecked")
    val seqDataArray = (List<Row>) row.get(3);
    assertThat(seqDataArray.get(0).length()).isEqualTo(3);
  }

  @Test
  public void callTest_wrongDataType() {
    function = new CreateRow(ExportType.SAMPLE, createStructType(), statsCalculator);
    val sourceNode = $("{_project_id:1,_sample_id:'SA000002',available_raw_sequence_data:["
        + "{raw_data_accession:'123',repository:'EGA',library_strategy:'WGS'}]}");

    val row = function.call(sourceNode);
    val projectId = row.get(0);
    assertThat(projectId).isInstanceOf(String.class);
  }

  private static StructType createStructType() {
    val fields = Lists.<StructField> newArrayList();
    fields.add(DataTypes.createStructField("_project_id", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("_sample_id", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("analyzed_sample_id", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("available_raw_sequence_data",
        DataTypes.createArrayType(createSeqDataStructType()), true));

    return DataTypes.createStructType(fields);
  }

  private static StructType createSeqDataStructType() {
    val fields = Lists.<StructField> newArrayList();
    fields.add(DataTypes.createStructField("library_strategy", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("raw_data_accession", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("repository", DataTypes.StringType, true));

    return DataTypes.createStructType(fields);
  }

}
