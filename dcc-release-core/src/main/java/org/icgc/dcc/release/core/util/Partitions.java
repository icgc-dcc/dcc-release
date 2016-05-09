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
package org.icgc.dcc.release.core.util;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.AbstractJavaRDDLike;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class Partitions {

  /**
   * Constants.
   */
  public static final String PARTITION_NAME = "project_name";

  public static String getPartitionName(@NonNull String projectName) {
    return PARTITION_NAME + "=" + projectName;
  }

  /**
   * This method is usually called for performance improvements, where the {@code primaryRdd} is joined to the
   * {@code secondaryRdd} with the {@code leftOuterJoin}. Usually the {@code primaryRdd} is bigger than the
   * {@code secondaryRdd}, so we want to repartition to the {@code primaryRdd}. However, if the {@code primaryRdd} is
   * empty and the {@code secondaryRdd} is not, the result of the join produces an empty RDD. To avoid such a situation
   * this method returns the biggest partition count of the two RDDs.
   */
  public static int getPartitionsCount(@NonNull AbstractJavaRDDLike<?, ?> primaryRdd,
      @NonNull AbstractJavaRDDLike<?, ?> secondaryRdd) {
    val primaryPartitions = primaryRdd.partitions().size();
    val secondaryPartitions = secondaryRdd.partitions().size();
    val partitionsCount = primaryPartitions == 0 ? secondaryPartitions : primaryPartitions;
    if (partitionsCount == 0) {
      log.warn("Resolved partitions count to 0. It may cause an incorrect join of 2 RDDs.");
    }

    return partitionsCount;
  }

  public static int getPartitionsCount(@NonNull AbstractJavaRDDLike<?, ?> rdd) {
    val partitionsCount = rdd.partitions().size();
    if (partitionsCount == 0) {
      log.warn("Resolved partitions count to 0. It may cause an incorrect join of 2 RDDs.");
    }

    return partitionsCount;
  }

}
