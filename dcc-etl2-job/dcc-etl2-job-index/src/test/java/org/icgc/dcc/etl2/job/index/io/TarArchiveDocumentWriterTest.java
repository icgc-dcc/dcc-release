package org.icgc.dcc.etl2.job.index.io;

import static org.icgc.dcc.etl2.job.index.factory.JacksonFactory.newDefaultMapper;
import static org.icgc.dcc.etl2.job.index.model.DocumentType.DONOR_TYPE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.job.index.core.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class TarArchiveDocumentWriterTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testWrite() throws IOException {
    val indexName = "index-1";
    val archiveFile = tmp.newFile(indexName + ".tar");
    val outputStream = new FileOutputStream(archiveFile);

    @Cleanup
    val writer = new TarArchiveDocumentWriter(indexName, outputStream);

    log.info("Writing {}...", archiveFile);
    for (int i = 0; i < 10; i++) {
      writer.write(new Document(DONOR_TYPE, "DO" + i, getSource()));
    }

    log.info("Finished");
  }

  @SneakyThrows
  private ObjectNode getSource() {
    return (ObjectNode) newDefaultMapper().readTree(new File("src/test/resources/fixtures/working/donor"));
  }

}
