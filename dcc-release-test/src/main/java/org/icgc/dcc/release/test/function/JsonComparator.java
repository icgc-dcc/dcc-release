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
package org.icgc.dcc.release.test.function;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;

import java.util.Map.Entry;
import java.util.function.Consumer;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This class is used to compare actual results produced by a job to actual results. Implement the {@code compare()}
 * method to provide specific comparison logic.
 */
@Slf4j
public class JsonComparator implements Consumer<Entry<ObjectNode, ObjectNode>> {

  @Override
  public void accept(Entry<ObjectNode, ObjectNode> entry) {
    val actual = entry.getKey();
    val expected = entry.getValue();

    try {
      compare(actual, expected);
    } catch (AssertionError e) {
      val message = e.getMessage();

      log.info("Expected:    {}", expected);
      log.warn("Actual:      {}", actual);
      log.error("Difference: {}", message);

      throw e;
    }
  }

  protected void compare(ObjectNode actual, ObjectNode expected) {
    assertJsonEquals(expected, actual);
  }

}