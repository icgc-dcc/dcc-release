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
package org.icgc.dcc.release.job.id.task;

import lombok.NonNull;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.id.client.core.IdClientFactory;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.id.function.AddSurrogateSampleId;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.icgc.dcc.release.job.id.model.SampleID;
import org.icgc.dcc.release.job.id.parser.ExportStringParser;
import scala.reflect.ClassTag$;

import java.util.Map;

public class AddSurrogateSampleIdTask extends AddSurrogateIdTask {

  public AddSurrogateSampleIdTask(@NonNull IdClientFactory idClientFactory) {
    super(FileType.SAMPLE, FileType.SAMPLE_SURROGATE_KEY, idClientFactory);
  }

  @Override
  protected JavaRDD<ObjectNode> process(JavaRDD<ObjectNode> input) {
    return input.map(
            new AddSurrogateSampleId(
                    idClientFactory,
                    input.context().broadcast(
                            (new ExportStringParser<SampleID>()).parse(
                                    idClientFactory.create().getAllSampleIds().get(),
                                    fields -> Pair.of(new SampleID(fields.get(1), fields.get(2)), fields.get(0))
                            ),
                            ClassTag$.MODULE$.apply(Map.class))
            )
    );
  }

}