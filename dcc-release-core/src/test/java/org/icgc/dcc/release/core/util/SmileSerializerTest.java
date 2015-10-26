package org.icgc.dcc.release.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.Jackson.toObjectNode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.val;

public class SmileSerializerTest {

  SmileSerializer smileSerializer;
  Kryo kryo;

  @Before
  public void setUp() {
    smileSerializer = new SmileSerializer();
    kryo = new Kryo();
  }

  @Test
  public void roundTripTest() throws Exception {
    val outStream = new ByteArrayOutputStream();
    val output = new Output(outStream, 32);
    val sourceObj1 = toObjectNode("{\"a\":1}");
    val sourceObj3 = toObjectNode("{\"a\":12}");
    val sourceObj4 = toObjectNode("{\"a\":555}");

    smileSerializer.write(kryo, output, sourceObj1);
    smileSerializer.write(kryo, output, null);
    smileSerializer.write(kryo, output, sourceObj3);
    smileSerializer.write(kryo, output, sourceObj4);
    output.flush();

    val input = new Input(new ByteArrayInputStream(outStream.toByteArray()), 32);
    val targetObj1 = smileSerializer.read(kryo, input, ObjectNode.class);
    val targetObj2 = smileSerializer.read(kryo, input, ObjectNode.class);
    val targetObj3 = smileSerializer.read(kryo, input, ObjectNode.class);
    val targetObj4 = smileSerializer.read(kryo, input, ObjectNode.class);

    assertThat(sourceObj1).isEqualTo(targetObj1);
    assertThat(targetObj2).isNull();
    assertThat(sourceObj3).isEqualTo(targetObj3);
    assertThat(sourceObj4).isEqualTo(targetObj4);

  }

}
