/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.annotate.parser;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static lombok.AccessLevel.PRIVATE;

import java.util.List;
import java.util.regex.Pattern;

import lombok.NoArgsConstructor;
import lombok.val;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

@NoArgsConstructor(access = PRIVATE)
public final class ConsequenceTypeParser {

  /**
   * Multiple consequences are separated by '&'
   */
  private static final Pattern MULTI_CONSEQUENCE_PATTERN = Pattern.compile("(\\w+)\\&(\\w+)");
  private static final Splitter SPLITTER = Splitter.on('&');

  public static List<String> parse(String effectName) {
    checkState(!isNullOrEmpty(effectName), "Null or empty consequence type");

    val matcher = MULTI_CONSEQUENCE_PATTERN.matcher(effectName);
    if (matcher.find()) {
      val result = Lists.newArrayList(SPLITTER.split(effectName));

      return result;
    }

    return ImmutableList.of(effectName);
  }

}
