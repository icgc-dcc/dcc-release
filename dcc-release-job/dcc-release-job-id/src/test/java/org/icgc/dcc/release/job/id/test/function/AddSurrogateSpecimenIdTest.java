package org.icgc.dcc.release.job.id.test.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.job.id.function.AddSurrogateSampleId;
import org.icgc.dcc.release.job.id.function.AddSurrogateSpecimenId;
import org.icgc.dcc.release.job.id.model.SampleID;
import org.icgc.dcc.release.job.id.model.SpecimenID;
import org.icgc.dcc.release.job.id.test.function.mock.MockCaches;
import org.icgc.dcc.release.job.id.test.function.mock.MockIdClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by gguo on 6/11/17.
 */
public class AddSurrogateSpecimenIdTest {
    @Test
    public void testCall(){
        Broadcast<Map<SpecimenID, String>> broadcast = IdJobTestSuite.sc.broadcast(MockCaches.getInstance().getSpecimens());

        AddSurrogateSpecimenId specimen = new AddSurrogateSpecimenId(IdJobTestSuite.factory, broadcast);

        ObjectMapper mapper = new ObjectMapper();
        ObjectReader reader = mapper.reader(ObjectNode.class);
        try {
            int index = 99;
            String json = String.format("{\"specimen_id\":\"specimen_%d\", \"_project_id\":\"project_%d\"}", index, index);

            ObjectNode objectNode = reader.readValue(json);

            ObjectNode newObjectNode = specimen.call(objectNode);

            assertEquals(index, Long.parseLong( newObjectNode.get("_specimen_id").textValue()) );

            index = 200;
            json = String.format("{\"specimen_id\":\"specimen_%d\", \"_project_id\":\"project_%d\"}", index, index);

            objectNode = reader.readValue(json);
            newObjectNode = specimen.call(objectNode);
            assertEquals(((MockIdClient)IdJobTestSuite.factory.create()).getNext_id_specimen() - 1, Long.parseLong( newObjectNode.get("_specimen_id").textValue()) );

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
