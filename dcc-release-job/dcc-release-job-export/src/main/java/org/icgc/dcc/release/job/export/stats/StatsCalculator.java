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
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

import java.io.Serializable;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.Accumulator;
import org.icgc.dcc.release.job.export.model.ExportType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

@RequiredArgsConstructor
public class StatsCalculator implements Serializable {

  @NonNull
  protected final ExportType type;
  @NonNull
  protected final Accumulator<Table<String, ExportType, Long>> accumulator;

  public void calculate(@NonNull ObjectNode row) {
    Table<String, ExportType, Long> stats = HashBasedTable.create();
    val donorId = getDonorId(row);
    val donorStats = stats.row(donorId);
    increment(donorStats, type, Long.valueOf(1L));

    accumulator.add(stats);
  }

  protected void increment(Map<ExportType, Long> donorStats, ExportType type, long value) {
    val currentValue = firstNonNull(donorStats.get(type), Long.valueOf(0L));
    donorStats.put(type, currentValue + value);
  }

  protected long getFieldCount(ObjectNode row, String fieldName) {
    return row.has(fieldName) ? row.get(fieldName).size() : 0L;
  }

  protected void incrementTypeStats(ObjectNode row, Map<ExportType, Long> donorStats, ExportType type, String fieldName) {
    val fieldCount = getFieldCount(row, fieldName);
    if (fieldCount > 0L) {
      increment(donorStats, type, fieldCount);
    }
  }

  protected String getDonorId(ObjectNode row) {
    val donorId = textValue(row, DONOR_ID);

    return donorId;
  }

}
