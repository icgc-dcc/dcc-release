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
package org.icgc.dcc.release.job.fathmm.repository;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.AA_MUTATION;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.TRANSLATION_ID;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.Update;

/**
 * This is a Data Access Object for FatHMM on postgresql database
 */
public class FathmmRepository implements Closeable {

  /**
   * Constants.
   */
  private static final String WEIGHT_TYPE = "INHERITED";

  private final Handle handle;

  private final String cacheQuery =  "select * from \"DCC_CACHE\" where translation_id = :translationId and aa_mutation = :aaMutation";
  private final String sequenceQuery = "select a.* from \"SEQUENCE\" a, \"PROTEIN\" b where a.id = b.id and b.name = :translationId";
  private final String domainQuery =  "select * from \"DOMAINS\" where id=:sequenceId and :substitution between seq_begin and seq_end order by score";
  private final String probabilityQuery = "select a.*, b.* from \"PROBABILITIES\" a, \"LIBRARY\" b where a.id=b.id and a.id=:probId and a.position=:probPosition";
  private final String weightQuery = "select disease, other from \"WEIGHTS\" where id=:wid and type='%s'\\:\\:weights_type";
  private final String updateCache = "insert into \"DCC_CACHE\" (translation_id,  aa_mutation, score, prediction) values (:translationId, :aaChange, :score, :prediction)";

  public FathmmRepository(@NonNull String fathmmPostgresqlUri) {
    this(new DBI(fathmmPostgresqlUri).open());
  }

  public FathmmRepository(@NonNull DataSource dataSource) {
    this(DBI.open(dataSource));
  }

  private FathmmRepository(@NonNull Handle handle) {
    this.handle = handle;
  }

  @Override
  @SneakyThrows
  public void close() {
    handle.close();
  }

  public Map<String, Object> getFromCache(@NonNull String translationId, @NonNull String aaChange) {
    val cache = handle.createQuery(cacheQuery).bind(TRANSLATION_ID, translationId).bind(AA_MUTATION, aaChange).first();
    return cache;
  }

  public void updateCache(@NonNull String translationId, @NonNull String aaChange, @NonNull String score,
      @NonNull String prediction) {
    val rowsAffected = handle.createStatement(updateCache)
        .bind("translationId", translationId)
        .bind("aaChange", aaChange)
        .bind("score", score)
        .bind("prediction", prediction)
        .execute();

    checkState(rowsAffected == 1, "Failed to update cache");
  }

  public Map<String, Object> getSequence(@NonNull String translationId) {
    return handle.createQuery(sequenceQuery).bind(TRANSLATION_ID, translationId).first();
  }

  public Map<String, Object> getWeight(@NonNull String weightId, @NonNull String weights) {
    return handle.createQuery(format(weightQuery, WEIGHT_TYPE)).bind("wid", weightId).first();
  }

  public Map<String, Object> getUnweightedProbability(String sequenceId, int substitution) {
    return handle.createQuery(probabilityQuery).bind("probId", sequenceId).bind("probPosition", substitution).first();
  }

  public List<Map<String, Object>> getDomains(int sequenceId, int substitution) {
    return handle.createQuery(domainQuery).bind("sequenceId", sequenceId).bind("substitution", substitution).list();
  }

  public Map<String, Object> getProbability(@NonNull String hmm, @NonNull Integer residue) {
    return handle.createQuery(probabilityQuery).bind("probId", hmm).bind("probPosition", residue).first();
  }

}
