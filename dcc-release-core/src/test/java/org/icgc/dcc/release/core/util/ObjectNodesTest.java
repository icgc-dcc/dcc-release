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
package org.icgc.dcc.release.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.core.function.JsonNodes.$;
import static org.icgc.dcc.release.core.util.ObjectNodes.mergeObjects;
import lombok.val;

import org.junit.Test;

public class ObjectNodesTest {

  @Test
  public void testMergeObjects() throws Exception {
    val source = $("{nested:{age:20}}");
    val target = $("{id:'1', nested:{weight: 30} }");
    val result = mergeObjects(target, source);

    assertThat(result).hasSize(2);
    assertThat(result.get("id").textValue()).isEqualTo("1");

    val nested = result.get("nested");
    assertThat(nested.get("age").asInt()).isEqualTo(20);
    assertThat(nested.get("weight").asInt()).isEqualTo(30);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeObjects_duplicate() throws Exception {
    val source = $("{id:1}");
    val target = $("{id:'1'}");
    mergeObjects(target, source);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeObjects_duplicateArray() throws Exception {
    val source = $("{id:[1]}");
    val target = $("{id:[1]}");
    mergeObjects(target, source);
  }

}
