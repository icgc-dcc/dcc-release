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
package org.icgc.dcc.release.job.export.stats;

import static com.google.common.base.Objects.firstNonNull;
import static java.lang.Long.valueOf;
import lombok.NonNull;
import lombok.val;

import org.apache.spark.AccumulatorParam;
import org.icgc.dcc.release.job.export.model.ExportType;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class StatsAccumulator implements AccumulatorParam<Table<String, ExportType, Long>> {

  @Override
  public Table<String, ExportType, Long> addInPlace(Table<String, ExportType, Long> left,
      Table<String, ExportType, Long> right) {
    for (val rightCell : right.cellSet()) {
      val rowKey = rightCell.getRowKey();
      val columnKey = rightCell.getColumnKey();

      val currentValue = firstNonNull(left.get(rowKey, columnKey), valueOf(0L));
      val mergedValue = currentValue + rightCell.getValue();
      left.put(rowKey, columnKey, mergedValue);
    }

    return left;
  }

  @Override
  public Table<String, ExportType, Long> zero(@NonNull Table<String, ExportType, Long> table) {
    Table<String, ExportType, Long> zeroAccumulator = HashBasedTable.create();

    return zeroAccumulator;
  }

  @Override
  public Table<String, ExportType, Long> addAccumulator(Table<String, ExportType, Long> left,
      Table<String, ExportType, Long> right) {
    return addInPlace(left, right);
  }

}
