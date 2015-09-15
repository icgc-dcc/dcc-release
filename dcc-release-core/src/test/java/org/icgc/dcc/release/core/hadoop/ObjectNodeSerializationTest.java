package org.icgc.dcc.release.core.hadoop;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY;
import static org.apache.hadoop.io.SequenceFile.Writer.compression;
import static org.apache.hadoop.io.SequenceFile.Writer.file;
import static org.apache.hadoop.io.SequenceFile.Writer.keyClass;
import static org.apache.hadoop.io.SequenceFile.Writer.valueClass;
import static org.icgc.dcc.release.core.function.JsonNodes.MAPPER;
import static org.icgc.dcc.release.core.util.Stopwatches.createStarted;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@Ignore("prototyping")
public class ObjectNodeSerializationTest {

  @Test
  public void testSerialize() throws IOException {
    @Cleanup
    val writer = createWriter(new Path("/tmp/test.seq"));
    val iterator = readInput();

    val watch = createStarted();
    while (iterator.hasNext()) {
      val value = iterator.next();
      writer.append(NullWritable.get(), value);
    }

    log.info("Finished in {}", watch);
  }

  private Iterator<ObjectNode> readInput() throws IOException, JsonProcessingException {
    val reader = MAPPER.reader(ObjectNode.class);
    return reader.readValues(new File("***REMOVED***/Data/Genes/gene.json"));
  }

  private Writer createWriter(Path path) throws IOException {
    val conf = new Configuration();
    conf.set(IO_SERIALIZATIONS_KEY,
        WritableSerialization.class.getName() + "," + ObjectNodeSerialization.class.getName());

    return SequenceFile.createWriter(conf,
        file(path),
        keyClass(NullWritable.class),
        valueClass(ObjectNode.class),
        compression(CompressionType.BLOCK));
  }

}
