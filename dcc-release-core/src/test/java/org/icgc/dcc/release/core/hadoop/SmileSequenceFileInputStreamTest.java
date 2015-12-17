package org.icgc.dcc.release.core.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.core.util.JacksonFactory.READER;

import java.io.IOException;

import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class SmileSequenceFileInputStreamTest {

  private static final String INPUT_PATH = "src/test/resources/fixtures/data.seq";

  FileSystem fileSystem;
  SmileSequenceFileInputStream inputStream;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.getLocal(new Configuration());
    val file = new Path(INPUT_PATH);
    inputStream = new SmileSequenceFileInputStream(fileSystem.getConf(), file);
  }

  @After
  public void tearDown() throws IOException {
    inputStream.close();
  }

  @Test
  public void readTest() throws IOException {
    val iterator = READER.<ObjectNode> readValues(inputStream);
    while (iterator.hasNext()) {
      val value = iterator.next();
      assertThat(value.isObject()).isTrue();
    }
  }

}
