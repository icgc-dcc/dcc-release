package org.icgc.dcc.release.job.document.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NoArgsConstructor;
import lombok.val;
import org.icgc.dcc.release.job.document.model.Occurrence;

@NoArgsConstructor
public final class MutationAnnotationData {

    // Method for standard ObjectNode ssm
    public static void attachMinimum(ObjectNode ssm, ObjectNode clinvar, Iterable<ObjectNode> civic) {
        // ObjectMapper mapper used to create new nodes
        ObjectMapper mapper = new ObjectMapper();

        // Attach empty nodes used later on
        val clinical_significance = mapper.createObjectNode();
        val clinical_evidence = mapper.createObjectNode();
        ssm.set("clinical_significance", clinical_significance);
        ssm.set("clinical_evidence", clinical_evidence);

        // If there is clinvar data pass it through otherwise don't and get defaults
        if (clinvar == null) {
            attachClinvarData(ssm);
        } else {
            attachClinvarData(ssm, clinvar);
        }

        // If there is civic data pass it through otherwise don't and get defaults
        if (civic == null) {
            attachCivicData(ssm);
        } else {
            attachCivicData(ssm, civic);
        }
    }

    // Method for Occurrence type
    public static void attachMinimum(Occurrence occurrence, ObjectNode clinvar, Iterable<ObjectNode> civic) {

        // ObjectMapper mapper used to create new nodes
        ObjectMapper mapper = new ObjectMapper();

        // Attach empty nodes used later on
        val clinical_significance = mapper.createObjectNode();
        val clinical_evidence = mapper.createObjectNode();
        occurrence.setClinical_significance(clinical_significance);
        occurrence.setClinical_evidence(clinical_evidence);

        // If there is clinvar data pass it through otherwise don't and get defaults
        if (clinvar == null) {
            attachClinvarData(occurrence);
        } else {
            attachClinvarData(occurrence, clinvar);
        }

        // If there is civic data pass it through otherwise don't and get defaults
        if (civic == null) {
            attachCivicData(occurrence);
        } else {
            attachCivicData(occurrence, civic);
        }
    }

    /**
     * Attached default (empty/null) values if no clinvar data is passed in
     * @param ssm - object node
     */
    private static void attachClinvarData(ObjectNode ssm) {
        ((ObjectNode)ssm.get("clinical_significance")).set("clinvar", null);
    }

    /**
     * Attached default (empty/null) values if no clinvar data is passed in
     * @param occurrence - Occurrence object
     */
    private static void attachClinvarData(Occurrence occurrence) {
        occurrence.getClinical_significance().set("clinvar", null);
    }

    /**
     * Attaches passed in clinvar data to observation
     * @param ssm - object node
     * @param clinvar object node to populate minimal clinvar data
     */
    private static void attachClinvarData(ObjectNode ssm, ObjectNode clinvar) {
        // Clinvar field extraction
        val clinicalSignificance = clinvar.get("clinicalSignificance");

        // ObjectMapper mapper used to create new nodes
        ObjectMapper mapper = new ObjectMapper();
        val clinvarObj = mapper.createObjectNode();
        clinvarObj.set("clinicalSignificance", clinicalSignificance);

        // Set fields
        ((ObjectNode)ssm.get("clinical_significance")).set("clinvar", clinvarObj);
    }

    /**
     * Attaches passed in clinvar data to observation
     * @param occurrence - Occurrence object
     * @param clinvar object node to populate minimal clinvar data
     */
    private static void attachClinvarData(Occurrence occurrence, ObjectNode clinvar) {

        // Clinvar field extraction
        val clinicalSignificance = clinvar.get("clinicalSignificance");

        // ObjectMapper mapper used to create new nodes
        ObjectMapper mapper = new ObjectMapper();
        val clinvarObj = mapper.createObjectNode();
        clinvarObj.set("clinicalSignificance", clinicalSignificance);

        // Set fields
        occurrence.getClinical_significance().set("clinvar", clinvarObj);
    }

    /**
     * Attached default (empty/null) values if no civic data is passed in
     * @param ssm - object node
     */
    private static void attachCivicData(ObjectNode ssm) {
        ((ObjectNode)ssm.get("clinical_evidence")).set("civic", null);
    }

    /**
     * Attached default (empty/null) values if no civic data is passed in
     * @param occurrence - Occurrence object
     */
    private static void attachCivicData(Occurrence occurrence) {
        occurrence.getClinical_evidence().set("civic", null);
    }


    /**
     * Attaches passed in civic data to observation
     * @param ssm - object node
     * @param civic iterable to populate civic data
     */
    private static void attachCivicData(ObjectNode ssm, Iterable<ObjectNode> civic) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode civicData = mapper.createArrayNode();
        civic.forEach(civicDataObj -> {
            ObjectNode civicObj = mapper.createObjectNode();
            civicObj.set("evidenceLevel", civicDataObj.get("evidenceLevel"));
            civicData.add(civicObj);
        });
        ((ObjectNode)ssm.get("clinical_evidence")).set("civic", civicData);
    }

    /**
     * Attaches passed in civic data to observation
     * @param occurrence - Occurrence object
     * @param civic iterable to populate civic data
     */
    private static void attachCivicData(Occurrence occurrence, Iterable<ObjectNode> civic) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode civicData = mapper.createArrayNode();
        civic.forEach(civicDataObj -> {
            ObjectNode civicObj = mapper.createObjectNode();
            civicObj.set("evidenceLevel", civicDataObj.get("evidenceLevel"));
            civicData.add(civicObj);
        });
        occurrence.getClinical_evidence().set("civic", civicData);
    }
}
