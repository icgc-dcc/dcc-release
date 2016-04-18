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
package org.icgc.dcc.release.job.export.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.release.job.export.model.ExportType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SchemaGenerator {

  private static final String SCHEMA_DEF_DIR = "/schemas";

  public StructType createDataType(ExportType exportType) {
    val fileSchemaDef = getFileSchemaDefinitions(exportType);
    val dataTypes = convertToDataType(fileSchemaDef);
    val fieldNames = getSortedFieldNames(exportType, fileSchemaDef);

    val structFields = fieldNames.stream()
        .map(f -> {
          DataType type = dataTypes.get(f);
          if (type == null) {
            ExportType childType = resolveChildExportType(f, exportType);
            type = DataTypes.createArrayType(createDataType(childType));
          }

          return DataTypes.createStructField(f, type, true);
        })
        .collect(toList());

    return DataTypes.createStructType(structFields);
  }

  private List<String> getSortedFieldNames(ExportType exportType, Map<String, String> fileSchema) {
    val fields = Lists.<String> newArrayList(fileSchema.keySet());
    fields.addAll(getChildFields(exportType));
    Collections.sort(fields);

    return fields;
  }

  private Map<String, DataType> convertToDataType(Map<String, String> schemaDef) {
    return schemaDef.entrySet().stream()
        .collect(toImmutableMap(e -> e.getKey(), e -> parseDataType(e.getValue())));
  }

  private Map<String, String> getFileSchemaDefinitions(ExportType exportType) {
    val schemaPath = getSchemaPath(exportType);
    val fileSchema = readFileSchemaDefinition(schemaPath);

    return fileSchema;
  }

  private static ExportType resolveChildExportType(String field, ExportType exportType) {
    val childType = exportType.getChildren().get(field);
    checkNotNull(childType, "Failed to resolve ExportType for child %s", field);

    return childType;
  }

  private static List<String> getChildFields(ExportType exportType) {
    return exportType.getChildren().entrySet().stream()
        .map(e -> e.getKey())
        .collect(toList());
  }

  private String getSchemaPath(ExportType exportType) {
    return SCHEMA_DEF_DIR + "/" + exportType.getId() + ".schema";
  }

  @SneakyThrows
  private Map<String, String> readFileSchemaDefinition(String schemaPath) {
    @Cleanup
    val reader = getFileSchemaDefinitionReader(schemaPath);
    String def = null;
    val schemaDef = Maps.<String, String> newHashMap();
    while ((def = reader.readLine()) != null) {
      val entry = parseSchemaDefinition(def);
      schemaDef.put(entry.getKey(), entry.getValue());
    }

    return schemaDef;
  }

  private BufferedReader getFileSchemaDefinitionReader(String schemaPath) {
    val in = getClass().getResourceAsStream(schemaPath);
    checkNotNull(in, "Failed to read schema definition %s", schemaPath);

    return new BufferedReader(new InputStreamReader(in));
  }

  private static Entry<String, String> parseSchemaDefinition(String schemaDefinition) {
    val keyValue = Lists.newArrayList(Splitters.COLON.split(schemaDefinition));
    checkState(keyValue.size() == 2, "Incorrect format of field definition. Expected <key> : <value>. Actual: %s",
        schemaDefinition);

    val fieldName = keyValue.get(0).trim();
    val value = keyValue.get(1).trim();

    return Maps.immutableEntry(fieldName, value);
  }

  private static DataType parseDataType(String dataTypeString) {
    switch (dataTypeString) {
    case "StringType":
      return DataTypes.StringType;
    case "BinaryType":
      return DataTypes.BinaryType;
    case "BooleanType":
      return DataTypes.BooleanType;
    case "DateType":
      return DataTypes.DateType;
    case "TimestampType":
      return DataTypes.TimestampType;
    case "CalendarIntervalType":
      return DataTypes.CalendarIntervalType;
    case "DoubleType":
      return DataTypes.DoubleType;
    case "ByteType":
      return DataTypes.ByteType;
    case "IntegerType":
      return DataTypes.IntegerType;
    case "LongType":
      return DataTypes.LongType;
    case "ShortType":
      return DataTypes.ShortType;
    case "NullType":
      return DataTypes.NullType;
    default:
      throw new IllegalArgumentException(format("Unknown datatype %s", dataTypeString));
    }
  }

}
