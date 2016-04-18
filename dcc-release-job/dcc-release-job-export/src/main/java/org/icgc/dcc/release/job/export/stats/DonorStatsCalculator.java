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

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;

import java.util.Map;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.Accumulator;
import org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames;
import org.icgc.dcc.release.job.export.model.ExportType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class DonorStatsCalculator extends StatsCalculator {

  public DonorStatsCalculator(Accumulator<Table<String, ExportType, Long>> accumulator) {
    super(ExportType.DONOR, accumulator);
  }

  @Override
  public void calculate(@NonNull ObjectNode row) {
    Table<String, ExportType, Long> stats = HashBasedTable.create();
    val donorId = getDonorId(row);
    val donorStats = stats.row(donorId);
    increment(donorStats, ExportType.DONOR, Long.valueOf(1L));

    incrementTypeStats(row, donorStats, ExportType.DONOR_THERAPY, JoinFieldNames.THERAPY);
    incrementTypeStats(row, donorStats, ExportType.DONOR_FAMILY, JoinFieldNames.FAMILY);
    incrementTypeStats(row, donorStats, ExportType.DONOR_EXPOSURE, JoinFieldNames.EXPOSURE);
    incrementTypeStats(row, donorStats, ExportType.SPECIMEN, DONOR_SPECIMEN);

    incrementSample(row, donorStats);

    accumulator.add(stats);
  }

  private void incrementSample(ObjectNode row, Map<ExportType, Long> donorStats) {
    if (row.has(DONOR_SPECIMEN) == false) {
      return;
    }

    long totalSamples = 0L;
    for (val specimen : row.get(DONOR_SPECIMEN)) {
      val samples = specimen.get(DONOR_SAMPLE);
      totalSamples += samples.size();
    }

    increment(donorStats, ExportType.SAMPLE, totalSamples);
  }

}
