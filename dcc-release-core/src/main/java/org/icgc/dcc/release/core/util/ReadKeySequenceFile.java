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
package org.icgc.dcc.release.core.util;

import static org.icgc.dcc.release.core.util.JacksonFactory.createSmileObjectReader;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectReader;

public final class ReadKeySequenceFile<T> implements
    PairFunction<Tuple2<Text, BytesWritable>, String, T> {

  private final Class<T> clazz;
  private transient ObjectReader reader;

  public ReadKeySequenceFile(@NonNull Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public Tuple2<String, T> call(Tuple2<Text, BytesWritable> tuple) throws Exception {
    checkReader();

    val key = tuple._1.toString();
    T value = reader.readValue(tuple._2.copyBytes());

    return tuple(key, value);
  }

  private void checkReader() {
    if (reader == null) {
      reader = createSmileObjectReader(clazz);
    }
  }

}