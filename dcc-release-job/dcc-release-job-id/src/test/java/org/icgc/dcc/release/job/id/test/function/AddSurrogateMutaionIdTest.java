package org.icgc.dcc.release.job.id.test.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.job.id.function.AddSurrogateMutationId;
import org.icgc.dcc.release.job.id.model.MutationID;
import org.icgc.dcc.release.job.id.test.function.mock.MockCaches;
import org.icgc.dcc.release.job.id.test.function.mock.MockIdClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by gguo on 6/11/17.
 */
public class AddSurrogateMutaionIdTest {

    @Test
    public void testCall() {
        Broadcast<Map<MutationID, String>> broadcast = IdJobTestSuite.sc.broadcast(MockCaches.getInstance().getMutations());

        AddSurrogateMutationId mutation = new AddSurrogateMutationId(IdJobTestSuite.factory, broadcast);

        ObjectMapper mapper = new ObjectMapper();
        ObjectReader reader = mapper.reader(ObjectNode.class);
        try {
            int index = 56;
            String json = String.format("{\"chromosome\":\"chromosome_%d\", \"chromosome_start\":\"chromosomeStart_%d\", \"chromosome_end\":\"chromosomeEnd_%d\", \"mutation\":\"mutation_%d\", \"mutation_type\":\"mutationType_%d\", \"assembly_version\":\"GRCh37\"}", index, index, index, index, index);

            ObjectNode objectNode = reader.readValue(json);

            ObjectNode newObjectNode = mutation.call(objectNode);

            assertEquals(index, Long.parseLong( newObjectNode.get("_mutation_id").textValue()) );

            index = 200;
            json = String.format("{\"chromosome\":\"chromosome_%d\", \"chromosome_start\":\"chromosomeStart_%d\", \"chromosome_end\":\"chromosomeEnd_%d\", \"mutation\":\"mutation_%d\", \"mutation_type\":\"mutationType_%d\", \"assembly_version\":\"GRCh37\"}", index, index, index, index, index);

            objectNode = reader.readValue(json);
            newObjectNode = mutation.call(objectNode);
            assertEquals(((MockIdClient)IdJobTestSuite.factory.create()).getNext_id_mutation() - 1, Long.parseLong( newObjectNode.get("_mutation_id").textValue()) );



            json = String.format("{\"chromosome\":\"chromosome_%d\", \"chromosome_start\":\"chromosome_start_%d\", \"chromosome_end\":\"chromosome_end_%d\", \"mutation\":\"mutation_%d\", \"mutation_type\":\"mutation_type_%d\", \"assembly_version\":\"abc\"}", index, index, index, index, index);

            objectNode = reader.readValue(json);
            newObjectNode = mutation.call(objectNode);
            assertEquals(((MockIdClient)IdJobTestSuite.factory.create()).getNext_id_mutation() - 1, Long.parseLong( newObjectNode.get("_mutation_id").textValue()) );

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
