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
package org.icgc.dcc.release.client;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * This is a stub for development.
 */
public class Temp {

  /**
   * Release 18 name.
   */
  public static final String RELEASE_NAME = "ICGC18";

  /**
   * Release 18 project names
   */
  public static final List<String> PROJECT_NAMES = ImmutableList.<String> of(
      "ALL-US",
      "BLCA-CN",
      "BLCA-US",
      "BOCA-FR",
      "BOCA-UK",
      "BRCA-UK",
      "BRCA-US",
      "CESC-US",
      "CLLE-ES",
      "CMDI-UK",
      "COAD-US",
      "COCA-CN",
      "EOPC-DE",
      "ESAD-UK",
      "ESCA-CN",
      "GACA-CN",
      "GBM-US",
      "HNSC-US",
      "KIRC-US",
      "KIRP-US",
      "LAML-KR",
      "LAML-US",
      "LGG-US",
      "LIAD-FR",
      "LICA-FR",
      "LIHC-US",
      "LIHM-FR",
      "LINC-JP",
      "LIRI-JP",
      "LUAD-US",
      "LUSC-CN",
      "LUSC-KR",
      "LUSC-US",
      "MALY-DE",
      "NBL-US",
      "ORCA-IN",
      "OV-AU",
      "OV-US",
      "PAAD-US",
      "PACA-AU",
      "PACA-CA",
      "PACA-IT",
      "PAEN-AU",
      "PBCA-DE",
      "PRAD-CA",
      "PRAD-UK",
      "PRAD-US",
      "READ-US",
      "RECA-CN",
      "RECA-EU",
      "SKCM-US",
      "STAD-US",
      "THCA-SA",
      "THCA-US",
      "UCEC-US");

  /**
   * Submission dir.
   */
  public static final String RELEASE_DIR = "/icgc/submission/ICGC18";

  /**
   * Workspace dir.
   */
  public static final String STAGING_DIR = "/tmp/dcc-workflow";

}
