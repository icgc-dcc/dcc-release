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
package org.icgc.dcc.release.job.export.model;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableMap.of;
import static java.util.Collections.emptyMap;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.Identifiable;
import org.icgc.dcc.release.core.job.FileType;

@Getter
public enum ExportType implements Identifiable {

  BIOMARKER,
  DONOR_EXPOSURE,
  DONOR_FAMILY,
  AVAILABLE_RAW_SEQUENCE_DATA,
  SAMPLE(of("available_raw_sequence_data", AVAILABLE_RAW_SEQUENCE_DATA)),
  DONOR_THERAPY,
  SURGERY,
  SPECIMEN(of("sample", SAMPLE, "biomarker", BIOMARKER, "surgery", SURGERY)),
  DONOR(FileType.CLINICAL, of("exposure", DONOR_EXPOSURE, "family", DONOR_FAMILY, "therapy", DONOR_THERAPY,
      "specimen", SPECIMEN)),

  CNSM_CONSEQUENCE,
  CNSM(FileType.CNSM, of("consequence", CNSM_CONSEQUENCE)),
  EXP_ARRAY(FileType.EXP_ARRAY),
  EXP_SEQ(FileType.EXP_SEQ),
  JCN(FileType.JCN),
  METH_ARRAY(FileType.METH_ARRAY),
  METH_SEQ(FileType.METH_SEQ, true, 40, 3),
  MIRNA_SEQ(FileType.MIRNA_SEQ),
  PEXP(FileType.PEXP),
  SGV_CONSEQUENCE,
  SGV(FileType.SGV, of("consequence", SGV_CONSEQUENCE), true, 20, 4),
  SSM_CONSEQUENCE,
  SSM_OBSERVATION,
  SSM(FileType.SSM, of("consequence", SSM_CONSEQUENCE, "observation", SSM_OBSERVATION)),
  STSM_CONSEQUENCE,
  STSM(FileType.STSM, of("consequence", STSM_CONSEQUENCE));

  private final FileType inputFileType;
  private final Map<String, ExportType> children;
  private final boolean splitByProject;
  private final int idPartitions;
  private final int parallelismMultiplier;

  private ExportType() {
    this(null, emptyMap(), false, 1, 1);
  }

  private ExportType(FileType inputFileType) {
    this(inputFileType, emptyMap(), false, 1, 1);
  }

  private ExportType(FileType inputFileType, boolean split) {
    this(inputFileType, emptyMap(), split, 1, 1);
  }

  private ExportType(FileType inputFileType, boolean split, int idPartitions, int parallelismMultiplier) {
    this(inputFileType, emptyMap(), split, idPartitions, parallelismMultiplier);
  }

  private ExportType(Map<String, ExportType> children) {
    this(null, children, false, 1, 1);
  }

  private ExportType(FileType inputFileType, Map<String, ExportType> children) {
    this(inputFileType, children, false, 1, 1);
  }

  private ExportType(FileType inputFileType, Map<String, ExportType> children, boolean splitByProject,
      int idPartitions, int parallelismMultiplier) {
    this.inputFileType = inputFileType;
    this.children = children;
    this.splitByProject = splitByProject;
    this.idPartitions = idPartitions;
    this.parallelismMultiplier = parallelismMultiplier;
  }

  /**
   * @return {@code ExportType}s which will be exported to own tables.
   */
  public static Collection<ExportType> getExportTypes() {
    return Arrays.stream(values())
        .filter(et -> et.getInputFileType() != null)
        .collect(toImmutableSet());
  }

  public static ExportType getChildType(@NonNull ExportType parent, @NonNull String fieldName) {
    val childType = parent.getChildren().get(fieldName);
    checkNotNull(childType, "Failed to resolve child type for field %s of type %s", fieldName, parent.getId());

    return childType;
  }

  @Override
  public String getId() {
    return name().toLowerCase();
  }

}
