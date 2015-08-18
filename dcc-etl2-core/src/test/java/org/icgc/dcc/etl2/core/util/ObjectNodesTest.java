package org.icgc.dcc.etl2.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl2.core.function.JsonNodes.$;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.mergeObjects;
import lombok.val;

import org.junit.Test;

public class ObjectNodesTest {

  @Test
  public void testMergeObjects() throws Exception {
    val source = $("{nested:{age:20}}");
    val target = $("{id:'1', nested:{weight: 30} }");
    val result = mergeObjects(target, source);

    assertThat(result).hasSize(2);
    assertThat(result.get("id").textValue()).isEqualTo("1");

    val nested = result.get("nested");
    assertThat(nested.get("age").asInt()).isEqualTo(20);
    assertThat(nested.get("weight").asInt()).isEqualTo(30);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeObjects_duplicate() throws Exception {
    val source = $("{id:1}");
    val target = $("{id:'1'}");
    mergeObjects(target, source);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeObjects_duplicateArray() throws Exception {
    val source = $("{id:[1]}");
    val target = $("{id:[1]}");
    mergeObjects(target, source);
  }

}
