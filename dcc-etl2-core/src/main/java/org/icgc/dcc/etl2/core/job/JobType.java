/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS AS IS AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl2.core.job;

import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;

import lombok.val;

import com.google.common.collect.ImmutableList;

public enum JobType {

  ANNOTATE,
  EXPORT,
  FATHMM,
  FI,
  ID,
  IMAGE,
  IMPORT,
  INDEX,
  JOIN,
  MASK,
  ORPHAN,
  STAGE,
  SUMMARIZE;

  public static Collection<JobType> getTopologicalSortOrder() {
    val order = ImmutableList.of(
        JobType.STAGE,
        JobType.MASK,
        JobType.ID,
        JobType.IMAGE,
        // This was removed in release ICGC19 (DCC-3xxx)
        // JobType.ORPHAN,
        JobType.ANNOTATE,
        JobType.JOIN,
        JobType.FATHMM,
        JobType.FI,
        JobType.IMPORT,
        JobType.EXPORT,
        JobType.SUMMARIZE,
        JobType.INDEX
        );

    checkState(order.size() == values().length);

    return order;
  }

}
