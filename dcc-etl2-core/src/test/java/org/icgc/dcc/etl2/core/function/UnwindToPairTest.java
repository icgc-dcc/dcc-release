package org.icgc.dcc.etl2.core.function;

import static com.google.common.collect.Iterables.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl2.core.function.JsonNodes.$;
import lombok.val;

import org.icgc.dcc.etl2.core.function.string.SelectField;
import org.junit.Test;

import scala.Tuple2;

public class UnwindToPairTest {

  private UnwindToPair<String, String> unwindToPair;

  @Test
  public void unwindTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[{id:5, type:'2'},{id:5, type:'3'}] }");
    unwindToPair = new UnwindToPair<String, String>("nested",
        new SelectField("id"),
        new SelectField("type"));

    val result = unwindToPair.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindDeepNestedTest() throws Exception {
    val sourceNode = $("{id:'1', three:{times:{nested:[{id:5, type:'2'},{id:5, type:'3'}] }}}");
    unwindToPair = new UnwindToPair<String, String>("three.times.nested",
        new SelectField("id"),
        new SelectField("type"));

    val result = unwindToPair.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindWithParentTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[{type:'2'},{type:'3'}] }");
    unwindToPair = new UnwindToPair<String, String>("nested",
        new SelectField("id"),
        new SelectField("type"),
        true);

    val result = unwindToPair.call(sourceNode);
    assertResult(result, "1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void unwindWithParentDuplicateTest() throws Exception {
    val sourceNode = $("{id:1, nested:[{id:2}] }");
    unwindToPair = new UnwindToPair<String, String>("nested", kf -> "", vf -> "", true);
    unwindToPair.call(sourceNode);
  }

  private static void assertResult(Iterable<Tuple2<String, String>> result, String tupleKey) {
    assertThat(result).hasSize(2);
    assertThat(get(result, 0)._1).isEqualTo(tupleKey);
    assertThat(get(result, 0)._2).isEqualTo("2");
    assertThat(get(result, 1)._1).isEqualTo(tupleKey);
    assertThat(get(result, 1)._2).isEqualTo("3");
  }

}
