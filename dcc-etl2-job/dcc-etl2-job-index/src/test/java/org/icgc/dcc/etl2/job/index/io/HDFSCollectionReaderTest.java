package org.icgc.dcc.etl2.job.index.io;

import static org.icgc.dcc.etl2.job.index.model.CollectionFields.collectionFields;

import java.io.IOException;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl2.job.index.model.CollectionFields;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

public class HDFSCollectionReaderTest {

  @Test
  public void testReadReleases() throws JsonProcessingException, IOException {
    val collectionDir = new Path("./src/test/resources/fixtures/working");
    val fileSystem = createLocalFileSystem();

    @Cleanup
    val reader = new HDFSCollectionReader(collectionDir, fileSystem);
    val releases = reader.readReleases(excludeFields("z", "w.b"));

    for (val release : releases) {
      System.out.println(release);
    }
  }

  @SneakyThrows
  private LocalFileSystem createLocalFileSystem() {
    return FileSystem.getLocal(new Configuration());
  }

  private static CollectionFields excludeFields(String... fieldNames) {
    return collectionFields().excludedFields(fieldNames).build();
  }

}
