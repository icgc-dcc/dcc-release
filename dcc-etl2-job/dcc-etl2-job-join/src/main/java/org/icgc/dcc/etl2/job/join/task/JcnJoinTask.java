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
package org.icgc.dcc.etl2.job.join.task;

import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_VERIFICATION_PLATFORM;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.BIOLOGICAL_VALIDATION_PLATFORM;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.JUNCTION_SEQ;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.JUNCTION_TYPE;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.OTHER_ANALYSIS_ALGORITHM;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.PROBABILITY;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.QUALITY_SCORE;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.SECOND_GENE_STABLE_ID;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.SEQ_COVERAGE;

import java.util.Map;

import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.job.join.model.SampleInfo;

public class JcnJoinTask extends PrimaryMetaJoinTask {

  private static final FileType PRIMARY_FILE_TYPE = FileType.JCN_P;

  private static final String[] REMOVE_FIELDS = {
      BIOLOGICAL_VALIDATION_PLATFORM,
      JUNCTION_SEQ,
      JUNCTION_TYPE,
      OTHER_ANALYSIS_ALGORITHM,
      PROBABILITY,
      QUALITY_SCORE,
      SECOND_GENE_STABLE_ID,
      SEQ_COVERAGE,
      OBSERVATION_VERIFICATION_PLATFORM };

  public JcnJoinTask(Broadcast<Map<String, Map<String, SampleInfo>>> donorSamples) {
    super(donorSamples, PRIMARY_FILE_TYPE, REMOVE_FIELDS);
  }

}
