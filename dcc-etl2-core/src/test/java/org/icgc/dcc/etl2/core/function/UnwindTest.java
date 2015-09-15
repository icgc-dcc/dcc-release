package org.icgc.dcc.etl2.core.function;

import static com.google.common.collect.Iterables.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl2.core.function.JsonNodes.$;
import static org.icgc.dcc.etl2.core.function.Unwind.unwind;
import static org.icgc.dcc.etl2.core.function.Unwind.unwindToParent;
import lombok.val;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class UnwindTest {

  private Unwind unwindFunction;

  @Test(expected = IllegalArgumentException.class)
  public void unwindNonArrayTest() throws Exception {
    val sourceNode = $("{id:'1', nested:5 }");
    unwindFunction = unwind("nested");
    unwindFunction.call(sourceNode);
  }

  @Test
  public void unwindTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[{id:5, type:'2'},{id:5, type:'3'}] }");
    unwindFunction = unwind("nested");

    val result = unwindFunction.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void noElementButINcludeParent() throws Exception {
    val sourceNode = $("{id:'1'}");
    unwindFunction = unwindToParent("three.times.nested");
    val result = unwindFunction.call(sourceNode);
    assertThat(result).hasSize(1);
    val node = get(result, 0);

    assertThat(node.get("id").textValue()).isEqualTo("1");
    assertThat(node.path("type").isMissingNode()).isTrue();
  }

  @Test
  public void unwindDeepNestedTest() throws Exception {
    val sourceNode = $("{id:'1', three:{times:{nested:[{id:5, type:'2'},{id:5, type:'3'}] }}}");
    unwindFunction = unwind("three.times.nested");

    val result = unwindFunction.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindMultiNestedWithMissingPathInOneOfTheObjectsTest() throws Exception {
    val sourceNode = $("{id:1,twice:[{nested:[{id:5,type:2},{id:5,type:3}]},{}]}");
    unwindFunction = unwind("twice.nested");

    val result = unwindFunction.call(sourceNode);
    assertResult(result, "5");
  }

  @Test
  public void unwindMissingWithParentTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[] }");
    unwindFunction = unwindToParent("nested");

    val result = unwindFunction.call(sourceNode);
    assertThat(result).hasSize(1);
    val node = get(result, 0);
    assertThat(node).hasSize(1);
    assertThat(node.get("id").textValue()).isEqualTo("1");
  }

  @Test
  public void unwindMutliNestedTest() throws Exception {
    val sourceNode =
        $("{id:1,three:[{times:[{nested:[{id:5,type:2},{id:5,type:3}]},{nested:[{id:5,type:2},{id:5,type:3}]}]},{times:[{nested:[{id:5,type:2},{id:5,type:3}]},{nested:[{id:5,type:2},{id:5,type:3}]}]}]}");
    unwindFunction = unwind("three.times.nested");

    val result = ImmutableList.copyOf(unwindFunction.call(sourceNode));
    assertThat(result).hasSize(8);
    for (int i = 0; i < result.size(); i += 2) {
      assertResult(ImmutableList.of(result.get(i), result.get(i + 1)), "5");
    }
  }

  @Test
  public void unwindWithParentTest() throws Exception {
    val sourceNode = $("{id:'1', nested:[{type:'2'},{type:'3'}] }");
    unwindFunction = unwindToParent("nested");

    val result = unwindFunction.call(sourceNode);
    assertResult(result, "1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void unwindWithParentDuplicateTest() throws Exception {
    val sourceNode = $("{id:1, nested:[{id:2}] }");
    unwindFunction = unwindToParent("nested");
    unwindFunction.call(sourceNode);
  }

  @Test
  public void unwindMissing() throws Exception {
    val sourceNode = $("{id:'1'}");
    unwindFunction = unwind("nested");
    val result = unwindFunction.call(sourceNode);
    assertThat(result).isEmpty();
  }

  @Test
  public void unwindMissingNested() throws Exception {
    val sourceNode = $("{id:'1'}");
    unwindFunction = unwind("three.time.nested");
    val result = unwindFunction.call(sourceNode);
    assertThat(result).isEmpty();
  }

  private static void assertResult(Iterable<ObjectNode> result, String keyValue) {
    assertThat(result).hasSize(2);
    assertResultObject(get(result, 0), keyValue, "2");
    assertResultObject(get(result, 1), keyValue, "3");
  }

  private static void assertResultObject(ObjectNode result, Object keyValue, Object typeValue) {
    assertThat(result.get("id").asText()).isEqualTo(keyValue);
    assertThat(result.get("type").asText()).isEqualTo(typeValue);
  }

}
