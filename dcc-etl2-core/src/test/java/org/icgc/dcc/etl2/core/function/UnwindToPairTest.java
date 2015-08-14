package org.icgc.dcc.etl2.core.function;

import static com.google.common.collect.Iterables.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl2.core.function.JsonNodes.$;
import static org.icgc.dcc.etl2.core.function.UnwindToPair.unwind;
import static org.icgc.dcc.etl2.core.function.UnwindToPair.unwindToParent;
import lombok.val;

import org.icgc.dcc.etl2.core.function.string.SelectField;
import org.junit.Test;

import scala.Tuple2;

public class UnwindToPairTest {

  private UnwindToPair<String, String> unwindToPair;

  @Test(expected = IllegalArgumentException.class)
  public void unwindNonArrayTest() throws Exception {
    val sourceNode = $("{id:'1', nested:5 }");
    unwindToPair = unwind("nested", new SelectField("id"), new SelectField("type"));
    unwindToPair.call(sourceNode);
  }

  @Test
  public void unwindTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[{id:5, type:'2'},{id:5, type:'3'}] }");
    unwindToPair = unwind("nested", new SelectField("id"), new SelectField("type"));

    val result = unwindToPair.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindDeepNestedTest() throws Exception {
    val sourceNode = $("{id:'1', three:{times:{nested:[{id:5, type:'2'},{id:5, type:'3'}] }}}");
    unwindToPair = unwind("three.times.nested", new SelectField("id"), new SelectField("type"));

    val result = unwindToPair.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindWithParentTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[{type:'2'},{type:'3'}] }");
    unwindToPair = unwindToParent("nested", new SelectField("id"), new SelectField("type"));

    val result = unwindToPair.call(sourceNode);
    assertResult(result, "1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void unwindWithParentDuplicateTest() throws Exception {
    val sourceNode = $("{id:1, nested:[{id:2}] }");
    unwindToPair = unwindToParent("nested", kf -> "", vf -> "");
    unwindToPair.call(sourceNode);
  }

  @Test
  public void unwindMissing() throws Exception {
    val sourceNode = $("{id:'1'}");
    unwindToPair = unwind("nested", new SelectField("id"), new SelectField("type"));
    val result = unwindToPair.call(sourceNode);
    assertThat(result).isEmpty();
  }

  @Test
  public void unwindMissingNested() throws Exception {
    val sourceNode = $("{id:'1'}");
    unwindToPair = unwind("three.time.nested", new SelectField("id"), new SelectField("type"));
    val result = unwindToPair.call(sourceNode);
    assertThat(result).isEmpty();
  }

  private static void assertResult(Iterable<Tuple2<String, String>> result, String tupleKey) {
    assertThat(result).hasSize(2);
    assertThat(get(result, 0)._1).isEqualTo(tupleKey);
    assertThat(get(result, 0)._2).isEqualTo("2");
    assertThat(get(result, 1)._1).isEqualTo(tupleKey);
    assertThat(get(result, 1)._2).isEqualTo("3");
  }

}
