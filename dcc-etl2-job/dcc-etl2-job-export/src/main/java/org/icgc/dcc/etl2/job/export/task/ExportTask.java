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
package org.icgc.dcc.etl2.job.export.task;

import static org.icgc.dcc.etl2.job.export.model.Constants.ClinicalDataFieldNames.ALL_FIELDS;
import static org.icgc.dcc.etl2.job.export.model.Constants.ClinicalDataFieldNames.DONOR_FIELDS;
import static org.icgc.dcc.etl2.job.export.model.Constants.ClinicalDataFieldNames.DONOR_FIELD_MAPPING;
import static org.icgc.dcc.etl2.job.export.model.Constants.ClinicalDataFieldNames.SPECIMEN_FIELD_MAPPING;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.function.FlattenField;
import org.icgc.dcc.etl2.core.function.ParseObjectNode;
import org.icgc.dcc.etl2.core.function.RenameFields;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.job.export.function.AddDonorIdField;
import org.icgc.dcc.etl2.job.export.function.AddMissingSpecimen;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.icgc.dcc.etl2.job.export.model.ExportTable;

@RequiredArgsConstructor
public class ExportTask {

  @NonNull
  private final JavaSparkContext sparkContext;
  @NonNull
  private final ExportTable table;

  protected JavaRDD<ObjectNode> process(Path inputPath) {
    return sparkContext
        .textFile(inputPath.toString())
        .map(new ParseObjectNode())
        .map(new RetainFields(DONOR_FIELDS))
        .map(new RenameFields(DONOR_FIELD_MAPPING))
        .map(new AddDonorIdField())
        .map(new AddMissingSpecimen())
        .flatMap(new FlattenField("specimen"))
        .map(new RetainFields(ALL_FIELDS))
        .map(new RenameFields(SPECIMEN_FIELD_MAPPING));
  }
}
