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
package org.icgc.dcc.etl2.core.function;

import static com.google.common.collect.Iterables.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl2.core.function.JsonNodes.$;
import lombok.val;

import org.junit.Test;

public class FlattenFieldTest {

  @Test
  public void testFlattenField() throws Exception {
    val flattenField = new FlattenField("y");

    val input = $("{x: 1, y: [2, 3]}");
    val actual = flattenField.call(input);

    assertThat(actual).hasSize(2);

    val first = get(actual, 0);
    assertThat(first)
        .isNotSameAs(input)
        .isEqualTo($("{x: 1, y: 2}"));

    val second = get(actual, 1);
    assertThat(second)
        .isNotSameAs(input)
        .isEqualTo($("{x: 1, y: 3}"));
  }

  @Test
  public void testFlattenField2() throws Exception {
    val flattenField = new FlattenField("y");

    val input = $("{x: 1}");
    val actual = flattenField.call(input);

    assertThat(actual).hasSize(1);

    val first = get(actual, 0);
    assertThat(first).isEqualTo($("{x: 1}"));

  }

}
