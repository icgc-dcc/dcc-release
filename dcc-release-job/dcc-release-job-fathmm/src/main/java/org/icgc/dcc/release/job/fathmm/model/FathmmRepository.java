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
package org.icgc.dcc.release.job.fathmm.model;

import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.AA_MUTATION;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.TRANSLATION_ID;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

/**
 * This is a Data Access Object for FatHMM on postgresql database
 */
// TODO: remove? it was used in the refactored version of Fathmm predictor
public class FathmmRepository implements Closeable {

  @NonNull
  private DBI dbi;
  @NonNull
  private final Handle handle;

  private final Query<Map<String, Object>> cacheQuery;
  private final Query<Map<String, Object>> sequenceQuery;
  private final Query<Map<String, Object>> domainQuery;
  private final Query<Map<String, Object>> probabilityQuery;
  private final Query<Map<String, Object>> unweightedProbabilityQuery;

  public FathmmRepository(@NonNull String fathmmPostgresqlUri) {
    this.handle = new DBI(fathmmPostgresqlUri).open();

    // @formatter:off
    this.cacheQuery                 = handle.createQuery("select * from \"DCC_CACHE\" where translation_id = :translationId and aa_mutation = :aaMutation");
    this.sequenceQuery              = handle.createQuery("select a.* from \"SEQUENCE\" a, \"PROTEIN\" b where a.id = b.id and b.name = :translationId");
    this.domainQuery                = handle.createQuery("select * from \"DOMAINS\" where id=:sequenceId and :substitution between seq_begin and seq_end order by score");
    this.probabilityQuery           = handle.createQuery("select a.*, b.* from \"PROBABILITIES\" a, \"LIBRARY\" b where a.id=b.id and a.id=:hmm and a.position=:residue");
    this.unweightedProbabilityQuery = handle.createQuery("select a.*, b.* from \"PROBABILITIES\" a, \"LIBRARY\" b where a.id=b.id and a.id=:sequenceId and a.position=:substitution");
    // @formatter:on
  }

  @Override
  @SneakyThrows
  public void close() {
    handle.close();
  }

  public Map<String, Object> getFromCache(@NonNull String translationId, @NonNull String aaChange) {
    val cache = cacheQuery.bind(TRANSLATION_ID, translationId).bind(AA_MUTATION, aaChange).first();
    return cache;
  }

  public void updateCache(@NonNull String translationId, @NonNull String aaChange, @NonNull String score,
      @NonNull String prediction) {
    handle.execute("insert into \"DCC_CACHE\" (translation_id,  aa_mutation, score, prediction) values (?,?,?,?)",
        translationId, aaChange, score, prediction);
  }

  public Map<String, Object> getSequence(@NonNull String translationId) {
    val sequence = sequenceQuery.bind(TRANSLATION_ID, translationId).first();
    return sequence;
  }

  public Map<String, Object> getWeight(@NonNull String weightId, @NonNull String weights) {
    val weightQuery = createWeightQuery(weights);
    val probability = weightQuery.bind("wid", weightId).first();
    return probability;
  }

  public Map<String, Object> getUnweightedProbability(String sequenceId, int substitution) {
    return unweightedProbabilityQuery.bind("sequenceId", sequenceId).bind("substitution", substitution).first();
  }

  public List<Map<String, Object>> getDomains(int sequenceId, int substitution) {
    return domainQuery.bind("sequenceId", sequenceId).bind("substitution", substitution).list();
  }

  private Query<Map<String, Object>> createWeightQuery(String weights) {
    return handle.createQuery("select disease, other from \"WEIGHTS\" where id=:wid and type=:type\\:\\:weights_type")
        .bind("type",
            weights);
  }

  public Map<String, Object> getProbability(@NonNull String hmm, @NonNull Integer residue) {
    return probabilityQuery.bind("hmm", hmm).bind("residue", residue).first();
  }

}
