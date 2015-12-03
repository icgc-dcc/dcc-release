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
package org.icgc.dcc.release.job.stage.function;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.Collections;
import java.util.List;

import lombok.NonNull;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.submission.SubmissionFileSchema;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public final class CleanSensitiveFields implements Function<ObjectNode, ObjectNode> {

  /**
   * Don't clean SSM_P and SGV_P as their controlled fields required to run annotation.
   */
  private static final List<String> SKIP_FILE_TYPES = ImmutableList.of(FileType.SSM_P.getId(), FileType.SGV_P.getId(),
      FileType.SGV_M.getId());

  private final boolean skipCleanup;
  private final List<String> controlledFields;

  public CleanSensitiveFields(@NonNull SubmissionFileSchema schema) {
    skipCleanup = SKIP_FILE_TYPES.contains(schema.getName());

    if (skipCleanup) {
      controlledFields = Collections.emptyList();
    } else {
      controlledFields = schema.getFields().stream()
          .filter(f -> f.isControlled())
          .map(f -> f.getName())
          .collect(toImmutableList());
    }
  }

  @Override
  public ObjectNode call(ObjectNode row) throws Exception {
    if (!skipCleanup) {
      controlledFields.stream()
          .forEach(fieldName -> row.remove(fieldName));
    }

    return row;
  }

}