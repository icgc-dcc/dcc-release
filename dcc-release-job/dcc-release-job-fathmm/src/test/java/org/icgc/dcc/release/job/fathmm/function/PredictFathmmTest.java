package org.icgc.dcc.release.job.fathmm.function;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.AA_MUTATION;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.PREDICTION;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.SCORE;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.TRANSLATION_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.val;

import org.icgc.dcc.release.job.fathmm.core.FathmmPredictor;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class PredictFathmmTest {

  private static final double TOLERANCE = 0.1;
  private static final String JDBC_URL = format("jdbc:h2:mem;MODE=MySQL;INIT=runscript from '%s'",
      "src/test/resources/sql/fathmm.sql");

  private FathmmPredictor predictor;

  @Before
  public void setUp() {
    predictor = new FathmmPredictor(JDBC_URL);
  }

  @Test
  public void testExecute() {
    List<Map<String, String>> inputs = new ArrayList<Map<String, String>>() {

      {
        add(createFATHMM("ENSP00000386181", "Q119R", "-1.16", "TOLERATED"));
        add(createFATHMM("ENSP00000305077", "E407G", "0.32", "TOLERATED"));
        add(createFATHMM("ENSP00000395632", "E271G", "0.32", "TOLERATED"));
        add(createFATHMM("ENSP00000367263", "D1697V", "5.5", "TOLERATED"));
        add(createFATHMM("ENSP00000282388", "R211H", "0.6", "TOLERATED"));
        add(createFATHMM("ENSP00000262109", "A251V", "1.5", "TOLERATED"));
        add(createFATHMM("ENSP00000428635", "A157V", "1.47", "TOLERATED"));
        add(createFATHMM("ENSP00000446447", "L195R", "-2.68", "DAMAGING"));
        add(createFATHMM("ENSP00000356972", "L293R", "-2.68", "DAMAGING"));
      }
    };

    inputs.stream().forEach(input -> {
      Map<String, String> result = predict(input.get(TRANSLATION_ID), input.get(AA_MUTATION));
      assertThat(result.get(PREDICTION)).isEqualTo(input.get(PREDICTION));

      double inputScore = Double.parseDouble(input.get(SCORE));
      double resultScore = Double.parseDouble(result.get(SCORE));
      assertThat(Math.abs(resultScore - inputScore)).isLessThan(TOLERANCE);
    });
  }

  private Map<String, String> predict(String translationIdStr, String aaMutationStr) {
    return predictor.predict(translationIdStr, aaMutationStr);
  }

  private static Map<String, String> createFATHMM(String translationId, String aaMutation, String score,
      String prediction) {
    val fathmm = ImmutableMap.<String, String> builder();
    fathmm.put(TRANSLATION_ID, translationId);
    fathmm.put(AA_MUTATION, aaMutation);
    fathmm.put(SCORE, score);
    fathmm.put(PREDICTION, prediction);
    return fathmm.build();
  }

}
