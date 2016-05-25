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

import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class DataTypeUtils {

  public static boolean checkDataType(Object value, DataType dataType, String fieldName) {
    log.debug("Verifying field name: '{}', Value: '{}'. Expected type: '{}'", fieldName, value, dataType);
    if (value == null) {
      return true;
    }

    if (DataTypes.IntegerType.equals(dataType) && !(value instanceof Integer)) {
      reportWarn(fieldName, value, "Integer");

      return false;
    } else if (DataTypes.StringType.equals(dataType) && !(value instanceof String)) {

      reportWarn(fieldName, value, "String");
      return false;
    } else if (DataTypes.DoubleType.equals(dataType) && !(value instanceof Double)) {

      reportWarn(fieldName, value, "Double");
      return false;
    }

    return true;
  }

  public static Object convertValue(Object value, DataType dataType) {
    if (DataTypes.IntegerType.equals(dataType)) {
      return toInt(value);
    } else if (DataTypes.StringType.equals(dataType)) {
      return toString(value);
    } else if (DataTypes.DoubleType.equals(dataType)) {
      return toDouble(value);
    } else {
      throw new IllegalArgumentException(format("Unsupported type %s", dataType));
    }
  }

  public static Double toDouble(Object value) {
    return value == null ? null : Double.valueOf(toString(value));
  }

  public static Integer toInt(Object value) {
    return value == null ? null : Integer.valueOf(toString(value));
  }

  public static String toString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static void reportWarn(String fieldName, Object actualType, String expectedType) {
    // TODO: Currently it generates a lot of data. Switch to WARNING level once schemas are generated from the
    // dictionary
    log.debug("Field '{}' has incorrect value type '{}'. Expected: '{}'.", fieldName,
        actualType.getClass().getSimpleName(), expectedType);
  }

}
