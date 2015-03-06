package org.icgc.dcc.etl2.job.annotate.model;

import static com.google.common.base.Preconditions.checkState;
import lombok.NonNull;
import lombok.Value;
import lombok.Builder;

/**
 * A snpEff 3.6 representation.
 */
@Value
@Builder
public final class SnpEffect implements Comparable<SnpEffect> {

  /**
   * It's set when an snpEff annotation is missing effect importance or cds_mutation generation failed.
   */
  public static final String METADATA_DELIMITER = "[()]";
  public static final String METADATA_SUBFIELD_DELIMITER = "|";

  ConsequenceType consequenceType;
  EffectImpact impact;
  EffectFunctionalClass functionalClass;
  String codonChange;
  String aminoAcidChange;
  String aminoAcidLength;
  String geneName;
  String geneBiotype;
  String coding;
  String transcriptID;
  String exonID;
  String cancerID;
  ParseState parseState;

  @Override
  public int compareTo(SnpEffect otherEffect) {
    return consequenceType.compareTo(otherEffect.getConsequenceType());
  }

  /**
   * Checks if there are annotation parsing errors or warnings.
   */
  public boolean hasError() {
    checkState(parseState != null);

    return parseState.hasError();
  }

  public boolean containsAnyError(@NonNull ParseNotification... error) {
    return parseState.containsAnyError(error);
  }

}