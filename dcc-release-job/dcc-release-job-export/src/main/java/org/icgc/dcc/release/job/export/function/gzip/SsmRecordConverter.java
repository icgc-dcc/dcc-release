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
package org.icgc.dcc.release.job.export.function.gzip;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.release.core.function.Unwind.unwindToParent;
import static org.icgc.dcc.release.core.function.Unwind.unwindToParentWithEmpty;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.model.Marking;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class SsmRecordConverter extends DefaultRecordConverter {

  private final Collection<Marking> access;

  public SsmRecordConverter(DownloadDataType dataType, int maxPartitions, Collection<Marking> access) {
    super(dataType, maxPartitions);
    this.access = access;
  }

  @Override
  protected JavaRDD<ObjectNode> preprocess(JavaRDD<ObjectNode> input) {
    return input
        .flatMap(unwindToParent(FieldNames.LoaderFieldNames.OBSERVATION_ARRAY_NAME))
        .filter(filterSsm(access))
        .flatMap(unwindToParentWithEmpty(FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME));
  }

  private static Function<ObjectNode, Boolean> filterSsm(Collection<Marking> access) {
    return row -> {
      String markingValue = textValue(row, FieldNames.NormalizerFieldNames.NORMALIZER_MARKING);

      Optional<Marking> resolvedMarking = Marking.from(markingValue);
      checkState(resolvedMarking.isPresent(), "Failed to resolve marking from value '%s'", markingValue);

      return access.contains(resolvedMarking.get());

    };
  }

}
