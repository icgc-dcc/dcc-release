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
package org.icgc.dcc.release.core.function;

import static com.google.common.collect.Iterables.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.core.function.JsonNodes.$;
import static org.icgc.dcc.release.core.function.Unwind.unwind;
import static org.icgc.dcc.release.core.function.Unwind.unwindToParent;
import static org.icgc.dcc.release.core.function.Unwind.unwindToParentWithEmpty;
import lombok.val;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class UnwindTest {

  private Unwind unwindFunction;

  @Test(expected = IllegalArgumentException.class)
  public void unwindNonArrayTest() throws Exception {
    val sourceNode = $("{id:'1', nested:5 }");
    unwindFunction = unwind("nested");
    unwindFunction.call(sourceNode);
  }

  @Test
  public void unwindTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[{id:5, type:'2'},{id:5, type:'3'}] }");
    unwindFunction = unwind("nested");

    val result = unwindFunction.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindDeepNestedTest() throws Exception {
    val sourceNode = $("{id:'1', three:{times:{nested:[{id:5, type:'2'},{id:5, type:'3'}] }}}");
    unwindFunction = unwind("three.times.nested");

    val result = unwindFunction.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindMultiNestedWithMissingPathInOneOfTheObjectsTest() throws Exception {
    val sourceNode = $("{id:1,twice:[{nested:[{id:5,type:2},{id:5,type:3}]},{}]}");
    unwindFunction = unwind("twice.nested");

    val result = unwindFunction.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindMissingWithParentTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[] }");
    unwindFunction = unwindToParent("nested");

    val result = unwindFunction.call(sourceNode);
    assertThat(result).isEmpty();
  }

  @Test
  public void unwindMissingToParentWithEmptyTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[] }");
    unwindFunction = unwindToParentWithEmpty("nested");

    val result = unwindFunction.call(sourceNode);
    assertThat(result).hasSize(1);
    val element = result.iterator().next();
    assertThat(element).isEqualTo($("{ id:'1' }"));
  }

  @Test
  public void unwindMutliNestedTest() throws Exception {
    val sourceNode =
        $("{id:1,three:[{times:[{nested:[{id:5,type:2},{id:5,type:3}]},{nested:[{id:5,type:2},{id:5,type:3}]}]},{times:[{nested:[{id:5,type:2},{id:5,type:3}]},{nested:[{id:5,type:2},{id:5,type:3}]}]}]}");
    unwindFunction = unwind("three.times.nested");

    val result = ImmutableList.copyOf(unwindFunction.call(sourceNode));
    assertThat(result).hasSize(8);
    for (int i = 0; i < result.size(); i += 2) {
      assertResult(ImmutableList.of(result.get(i), result.get(i + 1)), "5");
    }
  }

  @Test
  public void unwindWithParentTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[{type:'2'},{type:'3'}] }");
    unwindFunction = unwindToParent("nested");

    val result = unwindFunction.call(sourceNode);
    assertResult(result, "1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void unwindWithParentDuplicateTest() throws Exception {
    val sourceNode = $("{id:1, nested:[{id:2}] }");
    unwindFunction = unwindToParent("nested");
    unwindFunction.call(sourceNode);
  }

  @Test
  public void unwindMissing() throws Exception {
    val sourceNode = $("{id:'1'}");
    unwindFunction = unwind("nested");
    val result = unwindFunction.call(sourceNode);
    assertThat(result).isEmpty();
  }

  @Test
  public void unwindMissingNested() throws Exception {
    val sourceNode = $("{id:'1'}");
    unwindFunction = unwind("three.time.nested");
    val result = unwindFunction.call(sourceNode);
    assertThat(result).isEmpty();
  }

  private static void assertResult(Iterable<ObjectNode> result, String keyValue) {
    assertThat(result).hasSize(2);
    assertResultObject(get(result, 0), keyValue, "2");
    assertResultObject(get(result, 1), keyValue, "3");
  }

  private static void assertResultObject(ObjectNode result, Object keyValue, Object typeValue) {
    assertThat(result.get("id").asText()).isEqualTo(keyValue);
    assertThat(result.get("type").asText()).isEqualTo(typeValue);
  }

}
