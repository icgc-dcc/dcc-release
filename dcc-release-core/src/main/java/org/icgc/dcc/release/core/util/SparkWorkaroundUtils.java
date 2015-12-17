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

import java.util.Map;

import lombok.experimental.UtilityClass;

import com.google.common.collect.Maps;

@UtilityClass
public class SparkWorkaroundUtils {

  /**
   * Broadcast variabled don't work with all Map implementations.
   * @see <a
   * href="http://mail-archives.us.apache.org/mod_mbox/spark-user/201504.mbox/%3CCA+3qhFS0vXgJrfZ+e+yckpNPrm1wep8k=LSwEGNd53A7mPydzQ@mail.gmail.com%3E">Spark
   * discussion</a>
   */
  // TODO: Verify if this is fixed in the next Spark version. Current 1.5.2
  // FIXME: Raise a ticket
  public static <K, V> Map<K, V> toHashMap(Map<? extends K, ? extends V> map) {
    return Maps.newHashMap(map);
  }

}
