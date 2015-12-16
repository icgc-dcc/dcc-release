package org.icgc.dcc.release.core.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.core.util.JacksonFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class SequenceFileInputStreamTest {

  private static final String INPUT_PATH = "src/test/resources/fixtures/data.seq";

  FileSystem fileSystem;
  SequenceFileInputStream inputStream;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.getLocal(new Configuration());
    val file = new Path(INPUT_PATH);
    inputStream = new SequenceFileInputStream(fileSystem.getConf(), file);
  }

  @After
  public void tearDown() throws IOException {
    inputStream.close();
  }

  @Test
  public void readTest() throws IOException {
    val reader = JacksonFactory.MAPPER.reader(ObjectNode.class);
    val iterator = reader.<ObjectNode> readValues(inputStream);
    while (iterator.hasNext()) {
      val value = iterator.next();
      assertThat(value.isObject()).isTrue();
    }
  }

}
