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
package org.icgc.dcc.etl2.job.orphan.task;

import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.orphan.function.AddOrphanIndicator;
import org.icgc.dcc.etl2.job.orphan.model.Orphans;

@RequiredArgsConstructor
public class OrphanTask extends GenericTask {

  @NonNull
  private final Iterable<OrphanTaskDefinition> definitions;

  @Override
  public void execute(@NonNull TaskContext taskContext) {
    val orphans = resolveOrphans(taskContext);

    for (val definition : definitions) {
      writeOrphanedFileType(taskContext, definition, orphans);
    }
  }

  private Orphans resolveOrphans(TaskContext context) {
    return new OrphanResolver(context).resolveOrphans();
  }

  private void writeOrphanedFileType(TaskContext context, OrphanTaskDefinition definition, Orphans orphans) {
    val orphanedIds = orphans.getIds(definition.getInput());

    writeOrphanedFileType(context, definition.getInput(), definition.getOutput(), orphanedIds,
        definition.getIdFieldName());
  }

  private void writeOrphanedFileType(TaskContext context, FileType inputFileType, FileType outputFileType,
      Set<String> orphanedIds, String idFieldName) {
    val input = readInput(context, inputFileType);

    val processed = input.map(new AddOrphanIndicator(idFieldName, orphanedIds));

    writeOutput(context, processed, outputFileType);
  }

}