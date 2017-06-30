package org.icgc.dcc.release.job.id.test.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.job.id.function.AddSurrogateSampleId;
import org.icgc.dcc.release.job.id.model.SampleID;
import org.icgc.dcc.release.job.id.test.mock.MockCaches;
import org.icgc.dcc.release.job.id.test.mock.MockIdClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by gguo on 6/11/17.
 */
public class AddSurrogateSampleIdTest {

    @Test
    public void testCall() {
        Broadcast<Map<SampleID, String>> broadcast = IdJobTestSuite.sc.broadcast(MockCaches.getInstance().getSamples());

        AddSurrogateSampleId sample = new AddSurrogateSampleId(IdJobTestSuite.factory, broadcast);

        ObjectMapper mapper = new ObjectMapper();
        ObjectReader reader = mapper.reader(ObjectNode.class);
        try {
            int index = 77;
            String json = String.format("{\"analyzed_sample_id\":\"SA%d\", \"_project_id\":\"project_%d\"}", index, index);

            ObjectNode objectNode = reader.readValue(json);

            ObjectNode newObjectNode = sample.call(objectNode);

            assertEquals("SA" + index, newObjectNode.get("_sample_id").textValue() );

            index = 200;
            json = String.format("{\"analyzed_sample_id\":\"SA%d\", \"_project_id\":\"project_%d\"}", index, index);

            objectNode = reader.readValue(json);
            newObjectNode = sample.call(objectNode);
            assertEquals("SA" + (((MockIdClient)IdJobTestSuite.factory.create()).getNext_id_sample() - 1), newObjectNode.get("_sample_id").textValue() );

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
