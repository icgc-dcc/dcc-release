package org.icgc.dcc.release.core.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.io.Files2;
import org.icgc.dcc.release.core.util.JacksonFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class SequenceFileInputStreamTest {

  private static final String INPUT_PATH = "src/test/resources/fixtures/project.seq";

  FileSystem fileSystem;
  SequenceFileInputStream inputStream;

  @Before
  @SneakyThrows
  public void setUp() {
    fileSystem = FileSystem.getLocal(new Configuration());
    val file = new Path(INPUT_PATH);
    inputStream = new SequenceFileInputStream(fileSystem.getConf(), file);
  }

  @After
  @SneakyThrows
  public void tearUp() {
    inputStream.close();
  }

  @Test
  @SneakyThrows
  public void readTest() {
    val iterator = JacksonFactory.READER.<ObjectNode> readValues(inputStream);
    int count = 0;
    while (iterator.hasNext()) {
      val value = iterator.next();
      assertThat(value.isObject()).isTrue();
      log.warn("{} - {}", ++count, value);
    }

  }

  @Test
  @Ignore
  @SneakyThrows
  public void tmp() {
    val reader = Files2.getCompressionAgnosticBufferedReader(SIZE_FILE_NAME);
    String line = null;
    int counter = 0;
    while ((line = reader.readLine()) != null) {
      val length = Integer.parseInt(line);
      byte[] bytes = new byte[length];
      inputStream.read(bytes);

      val node = JacksonFactory.READER.<ObjectNode> readValue(bytes);
      log.info("{}", node);
      counter++;
    }

    log.info("{}", counter);
  }

  private static final String SIZE_FILE_NAME = "/tmp/size.txt";

}
