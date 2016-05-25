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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.icgc.dcc.release.job.export.util.DataTypeUtils.checkDataType;
import static org.icgc.dcc.release.job.export.util.DataTypeUtils.convertValue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.common.json.Jackson;
import org.icgc.dcc.release.core.util.ReleaseException;
import org.icgc.dcc.release.job.export.model.ExportType;
import org.icgc.dcc.release.job.export.stats.StatsCalculator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Slf4j
@RequiredArgsConstructor
public final class CreateRow implements Function<ObjectNode, Row> {

  /**
   * Configuration.
   */
  @NonNull
  private final ExportType exportType;
  @NonNull
  private final StructType exportTypeSchema;
  @NonNull
  private final StatsCalculator statsCalculator;

  @Override
  public Row call(ObjectNode json) {
    statsCalculator.calculate(json);
    val map = convert(json);
    Row row = null;
    try {
      row = createRow(map, exportType, exportTypeSchema);
    } catch (IllegalArgumentException e) {
      log.error("Failed to convert row: {}", json);
      throw e;
    }

    return row;
  }

  @SneakyThrows
  private Map<String, Object> convert(ObjectNode json) {
    val jsonString = json.toString();
    Map<String, Object> map = Maps.<String, Object> newHashMap();
    map = Jackson.DEFAULT.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

    return map;
  }

  private Row createRow(Map<String, Object> map, ExportType exportType, StructType exportTypeSchema) {
    val rowValues = Lists.newArrayList();
    val schemaFields = Lists.newArrayList(exportTypeSchema.fields());

    for (val field : schemaFields) {
      val fieldName = field.name();
      Object value = map.get(fieldName);
      val dataType = field.dataType();
      if (!checkDataType(value, dataType, fieldName)) {
        try {
          value = convertValue(value, dataType);
        } catch (NumberFormatException e) {
          val message = format("Failed to convert field %s to %s.", fieldName, dataType);
          log.error(message);
          throw new ReleaseException(message, e);
        }
      }

      if (value == null) {
        rowValues.add(null);
      } else if (isSimpleType(value)) {
        rowValues.add(value);
      } else {
        val childExportType = ExportType.getChildType(exportType, fieldName);
        val arrayType = (ArrayType) dataType;
        rowValues.add(convertArray(value, childExportType, (StructType) arrayType.elementType()));
      }
    }

    return create(rowValues);
  }

  private static Row create(List<? extends Object> rowValues) {
    return RowFactory.create(rowValues.toArray(new Object[rowValues.size()]));
  }

  private List<Row> convertArray(Object value, ExportType exportType, StructType exportTypeSchema) {
    checkState(value instanceof List);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> values = (List<Map<String, Object>>) value;

    List<Row> result = values.stream()
        .map(map -> createRow(map, exportType, exportTypeSchema))
        .collect(Collectors.toList());

    return result;
  }

  private static boolean isSimpleType(Object value) {
    return !(value instanceof List);
  }

}