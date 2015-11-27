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
package org.icgc.dcc.release.job.document.factory;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

@NoArgsConstructor(access = PRIVATE)
public final class JacksonFactory {

  /**
   * Mappers.
   */
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  private static final JsonFactory SMILE_FACTORY = new SmileFactory();
  private static final ObjectMapper SMILE_MAPPER = new ObjectMapper(SMILE_FACTORY);
  private static final ObjectReader SMILE_READER = SMILE_MAPPER.reader(ObjectNode.class);
  private static final ObjectWriter SMILE_WRITER = SMILE_MAPPER.writerWithType(ObjectNode.class);

  public static ObjectMapper newDefaultMapper() {
    return DEFAULT_MAPPER;
  }

  public static ObjectReader newSmileReader() {
    return SMILE_READER;
  }

  public static ObjectWriter newSmileWriter() {
    return SMILE_WRITER;
  }

}
