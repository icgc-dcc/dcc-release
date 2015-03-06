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
package org.icgc.dcc.etl2.test.util;

import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class TestFiles {

  private static final ObjectMapper MAPPER = new ObjectMapper().configure(AUTO_CLOSE_SOURCE, false);

  @SneakyThrows
  public static void writeInputFile(File source, File target) {
    val reader = MAPPER.reader(ObjectNode.class);

    @Cleanup
    val iterator = reader.readValues(source);

    @Cleanup
    val output = new PrintWriter(target);
    while (iterator.hasNext()) {
      val row = iterator.next();
      output.write(row.toString());
    }
  }

  @SneakyThrows
  public static void writeInputFile(List<ObjectNode> rows, File target) {
    @Cleanup
    val output = new PrintWriter(target);
    for (val row : rows) {
      output.write(row.toString());
    }
  }

  @SneakyThrows
  public static List<ObjectNode> readInputFile(File source) {
    val reader = MAPPER.reader(ObjectNode.class);

    @Cleanup
    MappingIterator<ObjectNode> iterator = reader.readValues(source);

    val rows = Lists.<ObjectNode> newArrayList();
    while (iterator.hasNext()) {
      val row = iterator.next();

      rows.add(row);
    }

    return rows;
  }

}
