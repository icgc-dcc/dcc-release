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
package org.icgc.dcc.release.job.annotate.function;

import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_VARIANT_ALLELE;
import static org.icgc.dcc.release.core.util.Mutations.createMutation;
import static org.icgc.dcc.release.job.annotate.model.AnnotatedFileType.SSM;

import java.util.Iterator;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.job.annotate.converter.ICGCToVCFConverter.MutationType;
import org.icgc.dcc.release.job.annotate.converter.SecondaryObjectNodeConverter;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.release.job.annotate.model.SecondaryEntity;
import org.icgc.dcc.release.job.annotate.snpeff.SnpEffPredictor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

@Slf4j
@RequiredArgsConstructor
public class SnpEffAnnotate implements FlatMapFunction<Iterator<ObjectNode>, ObjectNode> {

  /**
   * Returned instead of empty result produced by the Annotator.
   */
  public static final ObjectNode SENTINEL_VALUE = Jackson.DEFAULT.createObjectNode();

  private static final String MISSING_ALLELE = "-";

  /**
   * Configuration.
   */
  private final SnpEffProperties properties;
  @NonNull
  private final AnnotatedFileType fileType;

  @Override
  public Iterable<ObjectNode> call(final Iterator<ObjectNode> partition) {
    val predictor = new SnpEffPredictor(properties, fileType);

    log.info("Asynchronously forking SnpEff process...");
    predictor.start();
    log.info("Successfully asynchronously forked SnpEff process");

    return new SnpEffAnnotateIterator(partition, fileType, predictor);
  }

  @RequiredArgsConstructor
  private static class SnpEffAnnotateIterator implements Iterable<ObjectNode>, Iterator<ObjectNode> {

    /**
     * Dependencies.
     */
    private final Iterator<ObjectNode> delegate;

    /**
     * State
     */
    @NonNull
    private final AnnotatedFileType fileType;
    private final SnpEffPredictor predictor;
    private final List<ObjectNode> results = Lists.newArrayList();

    @Override
    public Iterator<ObjectNode> iterator() {
      return this;
    };

    @Override
    @SneakyThrows
    public boolean hasNext() {
      try {
        if (results.isEmpty() && !delegate.hasNext()) {
          close();

          return false;
        } else {
          if (results.isEmpty()) {
            predict(delegate.next());
          }

          return !results.isEmpty();
        }
      } catch (Exception e) {
        close();
        throw e;
      }
    }

    private void close() throws InterruptedException {
      // Clean up
      log.info("Shutdown the forked SnpEff process...");
      predictor.stop();
      log.info("Successfully shutdown the forked SnpEff process");
    }

    @Override
    public ObjectNode next() {
      return results.remove(0);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove a " + getClass().getName() + " iterator");
    }

    public void predict(ObjectNode row) {
      // Determine the prediction
      val predictions = predict(row, predictor);
      postprocessEmptyResults(predictions);
      for (val prediction : predictions) {
        results.add(SecondaryObjectNodeConverter.convert(prediction, fileType));
      }
    }

    private void postprocessEmptyResults(List<SecondaryEntity> predictions) {
      if (predictions.isEmpty()) {
        results.add(SENTINEL_VALUE);
      }
    }

    private List<SecondaryEntity> predict(ObjectNode row, SnpEffPredictor predictor) {
      // Extract row values
      val chromosome = row.get(fileType.getChromosomeFieldName()).textValue();
      val start = row.get(fileType.getChromosomeStartFieldName()).asLong();
      val end = row.get(fileType.getChromosomeEndFieldName()).asLong();
      val mutation = getMutation(row, fileType);
      val type = MutationType.fromId(row.get(fileType.getMutationTypeFieldName()).textValue());
      val ref = row.get(fileType.getReferenceAlleleFieldName()).textValue();
      val reference = (ref.equals(MISSING_ALLELE)) ? "" : ref;
      val id = row.get(fileType.getObservationIdFieldName()).textValue();

      return predictor.predict(chromosome, start, end, mutation, type, reference, id);
    }

    private static String getMutation(ObjectNode row, AnnotatedFileType fileType) {
      if (fileType == SSM) {
        return row.get(SUBMISSION_MUTATION).textValue();
      }

      val mutatedFrom = row.get(fileType.getReferenceAlleleFieldName()).textValue();
      val mutatedTo = row.get(SUBMISSION_OBSERVATION_VARIANT_ALLELE).textValue();

      return createMutation(mutatedFrom, mutatedTo);
    }

  }

}