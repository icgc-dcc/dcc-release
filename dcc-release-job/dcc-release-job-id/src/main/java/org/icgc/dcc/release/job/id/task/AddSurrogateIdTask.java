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
package org.icgc.dcc.release.job.id.task;

import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.id.client.core.IdClientFactory;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericProcessTask;
import org.icgc.dcc.release.core.task.TaskContext;

import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AddSurrogateIdTask extends GenericProcessTask {

  /**
   * Constants.
   */
  private static final long MAX_INPUT_FILE_SIZE_MB = 32L;

  /**
   * Configuration.
   */
  protected final IdClientFactory idClientFactory;

  public AddSurrogateIdTask(FileType inputFileType, FileType outputFileType, IdClientFactory idClientFactory) {
    super(inputFileType, outputFileType);
    this.idClientFactory = idClientFactory;
  }

  @Override
  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext) {
    val conf = createJobConf(taskContext);

    return readInput(taskContext, conf, inputFileType, MAX_INPUT_FILE_SIZE_MB);
  }

}