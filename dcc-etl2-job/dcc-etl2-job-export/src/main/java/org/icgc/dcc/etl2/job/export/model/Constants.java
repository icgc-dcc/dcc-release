package org.icgc.dcc.etl2.job.export.model;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.etl2.core.function.FlattenField;
import org.icgc.dcc.etl2.core.function.Identity;
import org.icgc.dcc.etl2.core.function.RenameFields;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.job.export.function.AddMissingConsequence;
import org.icgc.dcc.etl2.job.export.function.isType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class Constants {

  public static final String DONOR_ID = "donor_id";
  public static final String ICGC_DONOR_ID = "icgc_donor_id";
  public static final String ICGC_DONOR_ID_PREFIX = "DO";

  public static final String CNSM_TYPE_FIELD_VALUE = "cnsm";
  public static final String EXP_ARRAY_TYPE_FIELD_VALUE = "exp_array";
  public static final String EXP_SEQ_TYPE_FIELD_VALUE = "exp_seq";
  public static final String JCN_TYPE_FIELD_VALUE = "jcn";

  public static final String SPECIMEN_FIELD_NAME = "specimen";
  public static final String CONSEQUENCE_FIELD_NAME = "consequence";

  public static final String EMPTY_SPECIMEN_VALUE =
      "[\"_specimen_id\":\"\",\"specimen_id\":\"\",\"specimen_type\":\"\",\"specimen_type_other\":\"\",\"specimen_interval\":\"\",\"specimen_donor_treatment_type\":\"\",\"specimen_donor_treatment_type_other\":\"\",\"specimen_processing\":\"\",\"specimen_processing_other\":\"\",\"specimen_storage\":\"\",\"specimen_storage_other\":\"\",\"tumour_confirmed\":\"\",\"specimen_biobank\":\"\",\"specimen_biobank_id\":\"\",\"specimen_available\":\"\",\"tumour_histological_type\":\"\",\"tumour_grading_system\":\"\",\"tumour_grade\":\"\",\"tumour_grade_supplemental\":\"\",\"tumour_stage_system\":\"\",\"tumour_stage\":\"\",\"tumour_stage_supplemental\":\"\",\"digital_image_of_stained_section\":\"\"]";

  public static final String EMPTY_CONSEQUENCE_VALUE = "";

  public static class CNSMDataFiledNames {

    private static final List<String> FIRST_LEVEL_FIELDS = Arrays.asList(
        "_donor_id",
        "_project_id",
        "_specimen_id",
        "_sample_id",
        "_matched_sample_id",
        "analyzed_sample_id",
        "matched_sample_id",
        "mutation_type",
        "copy_number",
        "segment_mean",
        "segment_median",
        "chromosome",
        "chromosome_start",
        "chromosome_end",
        "assembly_version",
        "chromosome_start_range",
        "chromosome_end_range",
        "start_probe_id",
        "end_probe_id",
        "sequencing_strategy",
        "quality_score",
        "probability",
        "is_annotated",
        "verification_status",
        "verification_platform",
        CONSEQUENCE_FIELD_NAME,
        "platform",
        "experimental_protocol",
        "base_calling_algorithm",
        "alignment_algorithm",
        "variation_calling_algorithm",
        "other_analysis_algorithm",
        "seq_coverage",
        "raw_data_repository",
        "raw_data_accession");

    private static final List<String> CONSEQUENCE_FIELDS = Arrays.asList(
        "gene_affected",
        "transcript_affected",
        "gene_build_version");

    private static final ImmutableMap<String, String> FIELD_MAPPING = ImmutableMap.<String, String> builder()
        .put("_donor_id", "icgc_donor_id")
        .put("_project_id", "project_code")
        .put("_specimen_id", "icgc_specimen_id")
        .put("_sample_id", "icgc_sample_id")
        .put("_matched_sample_id", "matched_icgc_sample_id")
        .put("analyzed_sample_id", "submitted_sample_id")
        .put("matched_sample_id", "submitted_matched_sample_id")
        .build();

    private static final List<String> ALL_FIELDS = Lists.newArrayList(
        Iterables.concat(FIRST_LEVEL_FIELDS, CONSEQUENCE_FIELDS, FIELD_MAPPING.values()));

    public static final Function<ObjectNode, Boolean> PRIMARY_TYPE_FILTER = new isType(CNSM_TYPE_FIELD_VALUE);
    public static final Function<ObjectNode, ObjectNode> PRIMARY_TYPE_FILTER_FIELDS = new RetainFields(
        FIRST_LEVEL_FIELDS);
    public static final Function<ObjectNode, ObjectNode> PRIMARY_TYPE_RENAME_FIELDS = new RenameFields(
        FIELD_MAPPING);
    public static final Function<ObjectNode, ObjectNode> SECONDARY_TYPE_ADD_MISSING = new AddMissingConsequence();

    public static final FlattenField SECONDARY_TYPE_FLATTEN = new FlattenField(CONSEQUENCE_FIELD_NAME);
    public static final Function<ObjectNode, ObjectNode> ALL_TYPE_FILTER_FIELDS = new RetainFields(ALL_FIELDS);

    public static final Function<ObjectNode, ObjectNode> SECONDARY_TYPE_RENAME_FIELDS = new Identity();

  }

  public static class ExpArrayDataFiledNames {

    private static final List<String> FIRST_LEVEL_FIELDS = Arrays.asList(
        "_donor_id",
        "_project_id",
        "_specimen_id",
        "_sample_id",
        "analyzed_sample_id",
        "analysis_id",
        "gene_model",
        "gene_id",
        "normalized_expression_value",
        "fold_change",
        "platform",
        "experimental_protocol",
        "normalization_algorithm",
        "other_analysis_algorithm",
        "raw_data_repository",
        "raw_data_accession",
        "reference_sample_type");

    private static final ImmutableMap<String, String> FIELD_MAPPING = ImmutableMap.<String, String> builder()
        .put("_donor_id", "icgc_donor_id")
        .put("_project_id", "project_code")
        .put("_specimen_id", "icgc_specimen_id")
        .put("_sample_id", "icgc_sample_id")
        .put("analyzed_sample_id", "submitted_sample_id")
        .build();

    // private static final List<String> ALL_FIELDS = Lists.newArrayList(
    // Iterables.concat(FIRST_LEVEL_FIELDS, FIELD_MAPPING.values()));

    public static final Function<ObjectNode, Boolean> PRIMARY_TYPE_FILTER = new isType(EXP_ARRAY_TYPE_FIELD_VALUE);

    public static final Function<ObjectNode, ObjectNode> PRIMARY_TYPE_FILTER_FIELDS = new RetainFields(
        FIRST_LEVEL_FIELDS);

    public static final Function<ObjectNode, ObjectNode> PRIMARY_TYPE_RENAME_FIELDS = new RenameFields(
        FIELD_MAPPING);

    // public static final Function<ObjectNode, ObjectNode> SECONDARY_TYPE_ADD_MISSING = new Identity();
    //
    // public static final FlatMapFunction<ObjectNode, ObjectNode> SECONDARY_TYPE_FLATTEN = new IdentityFlatMap();
    //
    // public static final Function<ObjectNode, ObjectNode> ALL_TYPE_FILTER_FIELDS = new RetainFields(ALL_FIELDS);
    //
    // public static final Function<ObjectNode, ObjectNode> SECONDARY_TYPE_RENAME_FIELDS = new Identity();

  }

  public static class ExpSeqDataFiledNames {

    private static final List<String> FIRST_LEVEL_FIELDS = Arrays.asList(
        "_donor_id",
        "_project_id",
        "_specimen_id",
        "_sample_id",
        "analyzed_sample_id",
        "analysis_id",
        "gene_model",
        "gene_id",
        "normalized_read_count",
        "raw_read_count",
        "fold_change",
        "assembly_version",
        "platform",
        "total_read_count",
        "experimental_protocol",
        "alignment_algorithm",
        "normalization_algorithm",
        "other_analysis_algorithm",
        "sequencing_strategy",
        "raw_data_repository",
        "raw_data_accession",
        "reference_sample_type");

    private static final ImmutableMap<String, String> FIELD_MAPPING = ImmutableMap.<String, String> builder()
        .put("_donor_id", "icgc_donor_id")
        .put("_project_id", "project_code")
        .put("_specimen_id", "icgc_specimen_id")
        .put("_sample_id", "icgc_sample_id")
        .put("analyzed_sample_id", "submitted_sample_id")
        .build();

    // private static final List<String> ALL_FIELDS = Lists.newArrayList(
    // Iterables.concat(FIRST_LEVEL_FIELDS, FIELD_MAPPING.values()));

    public static final Function<ObjectNode, Boolean> PRIMARY_TYPE_FILTER = new isType(EXP_SEQ_TYPE_FIELD_VALUE);

    public static final Function<ObjectNode, ObjectNode> PRIMARY_TYPE_FILTER_FIELDS = new RetainFields(
        FIRST_LEVEL_FIELDS);

    public static final Function<ObjectNode, ObjectNode> PRIMARY_TYPE_RENAME_FIELDS = new RenameFields(
        FIELD_MAPPING);

    // public static final Function<ObjectNode, ObjectNode> SECONDARY_TYPE_ADD_MISSING = new Identity();
    //
    // public static final FlatMapFunction<ObjectNode, ObjectNode> SECONDARY_TYPE_FLATTEN = new IdentityFlatMap();
    //
    // public static final Function<ObjectNode, ObjectNode> ALL_TYPE_FILTER_FIELDS = new RetainFields(ALL_FIELDS);
    //
    // public static final Function<ObjectNode, ObjectNode> SECONDARY_TYPE_RENAME_FIELDS = new Identity();

  }

}
