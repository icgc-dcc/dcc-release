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
package org.icgc.dcc.release.job.annotate.parser;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Separators.DASH;
import static org.icgc.dcc.common.core.util.Separators.DOT;
import static org.icgc.dcc.common.core.util.Separators.SLASH;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.release.job.annotate.model.ConsequenceType.UNKNOWN_CONSEQUENCE;
import static org.icgc.dcc.release.job.annotate.model.ConsequenceType.byId;
import static org.icgc.dcc.release.job.annotate.model.InfoHeaderField.ALLELE;
import static org.icgc.dcc.release.job.annotate.model.InfoHeaderField.CDNA_POSITION_INDEX;
import static org.icgc.dcc.release.job.annotate.model.InfoHeaderField.FEATURE_ID_INDEX;
import static org.icgc.dcc.release.job.annotate.model.InfoHeaderField.GENE_ID_INDEX;
import static org.icgc.dcc.release.job.annotate.model.InfoHeaderField.HGVS_C_INDEX;
import static org.icgc.dcc.release.job.annotate.model.InfoHeaderField.HGVS_P_INDEX;
import static org.icgc.dcc.release.job.annotate.parser.AminoAcidChangeParser.parseAminoAcidChange;

import java.util.List;
import java.util.regex.Pattern;

import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.release.job.annotate.model.ConsequenceType;
import org.icgc.dcc.release.job.annotate.model.InfoHeaderField;
import org.icgc.dcc.release.job.annotate.model.ParseNotification;
import org.icgc.dcc.release.job.annotate.model.ParseState;
import org.icgc.dcc.release.job.annotate.model.SnpEffect;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

/**
 * This class orchestrates the set of parsers that parse individual snpEff annotated fields.
 */
@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class SnpEffectParser {

  private static final Pattern CODON_CHANGE_PATTERN = Pattern.compile("c\\.(\\d+\\w>\\w)");

  /**
   * An annotation has this number of fields.
   */
  private static final int ANNOTATION_FIELDS_COUNT = 16;
  private static final Splitter ANNOTATION_SPLITTER = Splitters.PIPE;

  /**
   * Number of expected metadata fields annotated by snpEff
   */
  public static final int METADATA_FIELDS_COUNT = 11;

  /**
   * Number of expected metadata fields annotated by snpEff when an error or warning encountered
   */
  public static final int WARNING_OR_ERROR_METADATA_FIELDS_COUNT = 12;

  /**
   * Number of expected metadata fields annotated by snpEff when an error and warning encountered
   */
  public static final int WARNING_AND_ERROR_MEDATA_FIELDS_COUNT = 13;
  public static final SnpEffect MALFORMED_SNP_EFFECT = SnpEffect.builder().consequenceType(UNKNOWN_CONSEQUENCE).build();

  /**
   * Parses an annotated by snpEff effect.
   * 
   * @param effectAnnotation - complete effect annotation with name. E.g.
   * {@code "T|intergenic_region|MODIFIER|KSR1P1-IGKV1OR10-1|ENSG00000229485-ENSG00000237592|intergenic_region|ENSG00000229485-ENSG00000237592|||n.42652889G>T||||||"}
   */
  public static List<SnpEffect> parse(String effectAnnotation) {
    log.debug("Parsing effect annotation: {}", effectAnnotation);
    if (isNullOrEmpty(effectAnnotation)) {
      return ImmutableList.of(MALFORMED_SNP_EFFECT);
    }

    val metadata = ANNOTATION_SPLITTER.splitToList(effectAnnotation);
    if (metadata.size() != ANNOTATION_FIELDS_COUNT) {
      log.warn("Malformed SnpEff effect: {}", effectAnnotation);

      return ImmutableList.of(MALFORMED_SNP_EFFECT);
    }

    val effectName = metadata.get(InfoHeaderField.CONSEQUENCE_TYPE_INDEX.getFieldIndex());
    val result = new ImmutableList.Builder<SnpEffect>();
    for (val consequenceType : ConsequenceTypeParser.parse(effectName)) {
      if (!ConsequenceType.ignore(consequenceType)) {
        result.add(parseIndividualEffect(consequenceType, metadata));
      }
    }

    return result.build();
  }

  private static SnpEffect parseIndividualEffect(String consequenceType, List<String> metadata) {
    val result = SnpEffect.builder();
    val parseState = new ParseState();

    result.consequenceType(byId(consequenceType));
    parseWarningsAndErrors(parseState, metadata.get(InfoHeaderField.ERRORS_INDEX.getFieldIndex()));
    val geneID = metadata.get(GENE_ID_INDEX.getFieldIndex());
    if (!isSkipGene(geneID)) {
      result.geneID(geneID);
    }

    val transcriptID = metadata.get(FEATURE_ID_INDEX.getFieldIndex());
    if (!isSkipGene(transcriptID)) {
      result.transcriptID(parseTranscriptId(transcriptID));
    }

    result.codonChange(parseCodonChange(metadata.get(HGVS_C_INDEX.getFieldIndex())));
    result.aminoAcidChange(parseAminoAcidChange(metadata.get(HGVS_P_INDEX.getFieldIndex())));
    result.aminoAcidPosition(parseAminoAcidPosition(metadata.get(CDNA_POSITION_INDEX.getFieldIndex())));

    result.allele(metadata.get(ALLELE.getFieldIndex()));
    result.parseState(parseState);

    return result.build();
  }

  private static String parseAminoAcidPosition(String cdnaPosition) {
    if (isNullOrEmpty(cdnaPosition)) {
      return null;
    }

    val aaPositionEnd = cdnaPosition.indexOf(SLASH);
    checkState(aaPositionEnd != -1, "Malformed amino acid position/length: '%s'", cdnaPosition);

    return cdnaPosition.substring(0, aaPositionEnd);
  }

  /**
   * Removes version from the transcript ID if it has it.
   */
  static String parseTranscriptId(String transcriptID) {
    val transcriptEnd = transcriptID.indexOf(DOT);

    return transcriptEnd == -1 ? transcriptID : transcriptID.substring(0, transcriptEnd);
  }

  private static boolean isSkipGene(String geneID) {
    // Ranges of genes occur for intergenic_region consequence types. They should be skipped.
    return geneID == null || geneID.contains(DASH);
  }

  static String parseCodonChange(String codonChange) {
    if (isNullOrEmpty(codonChange)) {
      return null;
    }

    val matcher = CODON_CHANGE_PATTERN.matcher(codonChange);
    if (matcher.matches()) {
      return matcher.group(1);
    }

    return null;
  }

  /**
   * Checks if the annotation does not contain warnings and errors. If so, update {@code parseError} and
   * {@code errorCode} accordingly.
   */
  private static void parseWarningsAndErrors(ParseState error, String warningsAndErrors) {
    if (isNullOrEmpty(warningsAndErrors)) {
      return;
    }

    val errorsList = Splitters.SLASH.splitToList(warningsAndErrors);
    val parseNotifications = errorsList.stream()
        .map(ParseNotification::getById)
        .collect(toImmutableList());

    error.addAllErrors(parseNotifications);
  }

}
