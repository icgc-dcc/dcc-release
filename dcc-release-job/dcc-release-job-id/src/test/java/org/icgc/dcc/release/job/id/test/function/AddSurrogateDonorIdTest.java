package org.icgc.dcc.release.job.id.test.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.job.id.function.AddSurrogateDonorId;
import org.icgc.dcc.release.job.id.model.DonorID;
import org.icgc.dcc.release.job.id.test.function.mock.MockCaches;
import org.icgc.dcc.release.job.id.test.function.mock.MockIdClient;
import org.icgc.dcc.release.job.id.test.function.mock.MockIdClientFactory;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;

/**
 * Created by gguo on 6/9/17.
 */
public class AddSurrogateDonorIdTest {

    @Test
    public void testCall(){
        Broadcast<Map<DonorID, String>> broadcast = IdJobTestSuite.sc.broadcast(MockCaches.getInstance().getDonors());

        AddSurrogateDonorId donor = new AddSurrogateDonorId(IdJobTestSuite.factory, broadcast);

        ObjectMapper mapper = new ObjectMapper();
        ObjectReader reader = mapper.reader(ObjectNode.class);
        try {
            int index = 23;
            String json = String.format("{\"donor_id\":\"donor_%d\", \"_project_id\":\"project_%d\"}", index, index);

            ObjectNode objectNode = reader.readValue(json);

            ObjectNode newObjectNode = donor.call(objectNode);

            assertEquals(index, Long.parseLong( newObjectNode.get("_donor_id").textValue()) );

            index = 200;
            json = String.format("{\"donor_id\":\"donor_%d\", \"_project_id\":\"project_%d\"}", index, index);

            objectNode = reader.readValue(json);
            newObjectNode = donor.call(objectNode);
            assertEquals(((MockIdClient)IdJobTestSuite.factory.create()).getNext_id_donor() - 1, Long.parseLong( newObjectNode.get("_donor_id").textValue()) );

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
