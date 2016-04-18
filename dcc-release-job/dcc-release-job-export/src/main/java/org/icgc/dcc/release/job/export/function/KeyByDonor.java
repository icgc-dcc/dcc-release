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

import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import com.google.common.collect.Maps;

@RequiredArgsConstructor
public final class KeyByDonor implements Function<Row, String> {

  private final int partitionFactor;
  private final Map<String, Integer> idPartitions = Maps.newHashMap();
  private static final Integer DEFAULT_PARTITION = 1;

  @Override
  public String call(Row row) throws Exception {
    // Assumes that _donor_id is always first. What is the case as fields are sorted by name
    val id = row.getString(0);
    if (partitionFactor == 1) {
      return id;
    } else {
      return id + "#" + getNextPartition(id);
    }
  }

  private Integer getNextPartition(String id) {
    Integer currentPartition = idPartitions.get(id);
    if (currentPartition == null) {
      idPartitions.put(id, DEFAULT_PARTITION);

      return DEFAULT_PARTITION;
    }

    if (currentPartition >= partitionFactor) {
      currentPartition = DEFAULT_PARTITION;
    } else {
      ++currentPartition;
    }
    idPartitions.put(id, currentPartition);

    return currentPartition;
  }

}