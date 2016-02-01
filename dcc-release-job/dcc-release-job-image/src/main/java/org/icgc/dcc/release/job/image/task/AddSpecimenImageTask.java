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
package org.icgc.dcc.release.job.image.task;

import static org.icgc.dcc.release.core.job.FileType.SPECIMEN_SURROGATE_KEY_IMAGE;
import static org.icgc.dcc.release.core.util.Tasks.hasInput;

import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.image.function.AddSpecimenImage;

@Slf4j
@RequiredArgsConstructor
public class AddSpecimenImageTask extends GenericTask {

  private static final FileType INPUT_FILE_TYPE = FileType.SPECIMEN_SURROGATE_KEY;
  private final Map<String, String> specimenImageUrls;

  @Override
  public void execute(TaskContext taskContext) {
    if (!hasInput(taskContext, INPUT_FILE_TYPE)) {
      log.debug("[{}] No input for '{}'. Skipping...", getName(), INPUT_FILE_TYPE);
      return;
    }

    val input = readInput(taskContext, INPUT_FILE_TYPE);
    val processed = input.map(new AddSpecimenImage(specimenImageUrls, taskContext.getProjectName().get()));
    writeOutput(taskContext, processed, SPECIMEN_SURROGATE_KEY_IMAGE);
  }

}