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
package org.icgc.dcc.release.job.export.io;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.Splitters.HASHTAG;

import java.io.IOException;

import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.icgc.dcc.common.core.util.Joiners;

import com.google.common.collect.Lists;

public class ProjectDonorMultipleGzipOutputFormat<V extends Object> extends
    MultipleTextOutputFormat<String, V> {

  @Override
  protected String generateActualKey(String key, V value) {
    return null;
  }

  @Override
  protected String generateFileNameForKeyValue(String key, V value, String name) {
    val parts = Lists.<String> newArrayList();
    parts.addAll(HASHTAG.splitToList(key));
    checkState(parts.size() == 2, "Malformed key: '%s'", key);
    parts.add(name);

    return Joiners.PATH.join(parts);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws FileAlreadyExistsException,
      InvalidJobConfException, IOException {
  }

}
