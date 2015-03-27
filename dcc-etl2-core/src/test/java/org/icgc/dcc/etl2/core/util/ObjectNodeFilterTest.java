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
package org.icgc.dcc.etl2.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl2.core.function.JsonNodes.$;
import lombok.val;

import org.icgc.dcc.etl2.core.util.ObjectNodeFilter.FilterMode;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ObjectNodeFilterTest {

  // Test data
  // @formatter:off
  JsonNode root = $(
    "{" +
      "a: {" +
        "b: 1," +
        "c: [" +
          "{" +
            "d: 2," +
            "e: 3" +
          "}," +
          "{" +
            "d: 4," +
            "e: 5" +
          "}" +
        "]" +
      "} " +
    "}"
  );
  // @formatter:on

  @Test
  public void testExcludeFilter() {
    // @formatter:off

    // Apply all JsonPath filters listed to root
    filter(root, FilterMode.EXCLUDE,
      "a.b", 
      "a.c.d"
    );

    // Verify
    assertThat(root).isEqualTo($(
      "{" +
        "a: {" +
          "c: [" +
            "{" +
              "e: 3" +
            "}," +
            "{" +
              "e: 5" +
            "}" +
          "]" +
        "} " +
      "}"
    ));
    // @formatter:on
  }

  @Test
  public void testIncludeFilter() {
    // @formatter:off

    // Apply all JsonPath filters listed to root
    filter(root, FilterMode.INCLUDE,
      "a.b", 
      "a.c.d"
    );

    // Verify
    assertThat(root).isEqualTo($(
      "{" +
        "a: {" +
          "b: 1," +
          "c: [" +
            "{" +
              "d: 2" +
            "}," +
            "{" +
              "d: 4" +
            "}" +
          "]" +
        "} " +
      "}"
    ));
    // @formatter:on
  }

  @Test
  public void testExcludeNonLeafObjectFilter() {
    // Apply all JsonPath filters
    val root = $("{x:{y:1}}");
    filter(root, FilterMode.EXCLUDE, "x");

    // Verify
    assertThat(root).isEqualTo($("{}"));
  }

  @Test
  public void testIncludeNonLeafObjectFilter() {
    // Apply all JsonPath filters
    val root = $("{x:{y:1}}");
    filter(root, FilterMode.INCLUDE, "x");

    // Verify
    assertThat(root).isEqualTo($("{x:{y:1}}"));
  }

  @Test
  public void testExcludeLeafObjectFilter() {
    // Apply all JsonPath filters
    val root = $("{x:1,y:1}");
    filter(root, FilterMode.EXCLUDE, "x");

    // Verify
    assertThat(root).isEqualTo($("{y:1}"));
  }

  @Test
  public void testIncludeLeafObjectFilter() {
    // Apply all JsonPath filters
    val root = $("{x:1,y:1}");
    filter(root, FilterMode.INCLUDE, "x");

    // Verify
    assertThat(root).isEqualTo($("{x:1}"));
  }

  private static void filter(JsonNode root, FilterMode mode, String... fieldNames) {
    val filter = new ObjectNodeFilter(mode, Sets.newSet(fieldNames));

    filter.filter((ObjectNode) root);
  }

}
