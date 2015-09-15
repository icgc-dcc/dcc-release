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
package org.icgc.dcc.release.job.export.model;

import static lombok.AccessLevel.PRIVATE;

import java.util.Arrays;
import java.util.stream.Stream;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.release.job.export.model.type.CNSM;
import org.icgc.dcc.release.job.export.model.type.Clinical;
import org.icgc.dcc.release.job.export.model.type.Donor;
import org.icgc.dcc.release.job.export.model.type.DonorExposure;
import org.icgc.dcc.release.job.export.model.type.DonorFamily;
import org.icgc.dcc.release.job.export.model.type.DonorTherapy;
import org.icgc.dcc.release.job.export.model.type.ExpArray;
import org.icgc.dcc.release.job.export.model.type.ExpSeq;
import org.icgc.dcc.release.job.export.model.type.JCN;
import org.icgc.dcc.release.job.export.model.type.MethArray;
import org.icgc.dcc.release.job.export.model.type.MethSeq;
import org.icgc.dcc.release.job.export.model.type.MirnaSeq;
import org.icgc.dcc.release.job.export.model.type.PExp;
import org.icgc.dcc.release.job.export.model.type.SGVControlled;
import org.icgc.dcc.release.job.export.model.type.SSMControlled;
import org.icgc.dcc.release.job.export.model.type.SSMOpen;
import org.icgc.dcc.release.job.export.model.type.STSM;
import org.icgc.dcc.release.job.export.model.type.Sample;
import org.icgc.dcc.release.job.export.model.type.Specimen;
import org.icgc.dcc.release.job.export.model.type.Type;

@Getter
@RequiredArgsConstructor(access = PRIVATE)
public enum ExportTable {

  CLINICAL("CLINICAL", "clinical", false, new Clinical()),
  CNSM("CNSM", "cnsm", false, new CNSM()),
  DONOR("DONOR", "donor", false, new Donor()),
  DONOR_EXPOSURE("DONOR_EXPOSURE", "donor_exposure", false, new DonorExposure()),
  DONOR_FAMILY("DONOR_FAMILY", "donor_family", false, new DonorFamily()),
  DONOR_THERAPY("DONOR_THERAPY", "donor_therapy", false, new DonorTherapy()),
  EXP_ARRAY("EXP_ARRAY", "exp_array", false, new ExpArray()),
  EXP_SEQ("EXP_SEQ", "exp_seq", false, new ExpSeq()),
  JCN("JCN", "jcn", false, new JCN()),
  METH_ARRAY("METH_ARRAY", "meth_array", false, new MethArray()),
  METH_SEQ("METH_SEQ", "meth_seq", false, new MethSeq()),
  MIRNA_SEQ("MIRNA_SEQ", "mirna_seq", false, new MirnaSeq()),
  PEXP("PEXP", "pexp", false, new PExp()),
  SAMPLE("SAMPLE", "sample", false, new Sample()),
  SGV_CONTROLLED("SGV_CONTROLLED", "sgv_controlled", true, new SGVControlled()),
  SPECIMEN("SPECIMEN", "specimen", false, new Specimen()),
  SSM_CONTROLLED("SSM_CONTROLLED", "ssm_controlled", true, new SSMControlled()),
  SSM_OPEN("SSM_OPEN", "ssm_open", false, new SSMOpen()),
  STSM("STSM", "stsm", false, new STSM());

  public final String name;

  public final String indexName;

  public final boolean isControlled;

  public Type type;

  private ExportTable(String name, String indexName,
      boolean isControlled, Type type) {
    this.name = name;
    this.indexName = indexName;
    this.isControlled = isControlled;
    this.type = type;
  }

  public static Stream<ExportTable> stream() {
    return Arrays.stream(values());
  }

}
