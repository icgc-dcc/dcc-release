package org.icgc.dcc.etl2.job.export.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class Constants {

  public static final String DONOR_ID = "donor_id";
  public static final String ICGC_DONOR_ID = "icgc_donor_id";
  public static final String ICGC_DONOR_ID_PREFIX = "DO";

  public static final String CNSM_TYPE = "CNSM";

  public static final String SPECIMEN_FIELD_NAME = "specimen";
  public static final String CONSEQUENCE_FIELD_NAME = "consequence";

  public static final String EMPTY_SPECIMEN_VALUE =
      "[\"_specimen_id\":\"\",\"specimen_id\":\"\",\"specimen_type\":\"\",\"specimen_type_other\":\"\",\"specimen_interval\":\"\",\"specimen_donor_treatment_type\":\"\",\"specimen_donor_treatment_type_other\":\"\",\"specimen_processing\":\"\",\"specimen_processing_other\":\"\",\"specimen_storage\":\"\",\"specimen_storage_other\":\"\",\"tumour_confirmed\":\"\",\"specimen_biobank\":\"\",\"specimen_biobank_id\":\"\",\"specimen_available\":\"\",\"tumour_histological_type\":\"\",\"tumour_grading_system\":\"\",\"tumour_grade\":\"\",\"tumour_grade_supplemental\":\"\",\"tumour_stage_system\":\"\",\"tumour_stage\":\"\",\"tumour_stage_supplemental\":\"\",\"digital_image_of_stained_section\":\"\"]";

  public static final String EMPTY_CONSEQUENCE_VALUE = "";

  public static final Map<DataType, Map<String, List<String>>> FIELDS =
      new HashMap<DataType, Map<String, List<String>>>();

  public static class ClinicalDataFieldNames {

    public static final List<String> DONOR_FIELDS = Arrays.asList(
        "_donor_id",
        "_project_id",
        "donor_id",
        "donor_sex",
        "donor_vital_status",
        "disease_status_last_followup",
        "donor_relapse_type",
        "donor_age_at_diagnosis",
        "donor_age_at_enrollment",
        "donor_age_at_last_followup",
        "donor_relapse_interval",
        "donor_diagnosis_icd10",
        "donor_tumour_staging_system_at_diagnosis",
        "donor_tumour_stage_at_diagnosis",
        "donor_tumour_stage_at_diagnosis_supplemental",
        "donor_survival_time",
        "donor_interval_of_last_followup",
        "specimen");

    public static final List<String> SPECIMEN_FIELDS = Arrays.asList(
        "_specimen_id",
        "specimen_id",
        "specimen_type",
        "specimen_type_other",
        "specimen_interval",
        "specimen_donor_treatment_type",
        "specimen_donor_treatment_type_other",
        "specimen_processing",
        "specimen_processing_other",
        "specimen_storage",
        "specimen_storage_other",
        "tumour_confirmed",
        "specimen_biobank",
        "specimen_biobank_id",
        "specimen_available",
        "tumour_histological_type",
        "tumour_grading_system",
        "tumour_grade",
        "tumour_grade_supplemental",
        "tumour_stage_system",
        "tumour_stage",
        "tumour_stage_supplemental",
        "digital_image_of_stained_section",
        "percentage_cellularity",
        "level_of_cellularity");

    public static final ImmutableMap<String, String> DONOR_FIELD_MAPPING = ImmutableMap.<String, String> builder()
        .put("_donor_id", "icgc_donor_id")
        .put("_project_id", "project_code")
        .put("donor_id", "submitted_donor_id")
        .build();

    public static final ImmutableMap<String, String> SPECIMEN_FIELD_MAPPING = ImmutableMap.<String, String> builder()
        .put("_specimen_id", "icgc_specimen_id")
        .put("specimen_id", "submitted_specimen_id")
        .build();

    public static final List<String> ALL_FIELDS = Lists.newArrayList(
        Iterables.concat(DONOR_FIELDS, SPECIMEN_FIELDS, DONOR_FIELD_MAPPING.values(), SPECIMEN_FIELD_MAPPING.values()));

  }

  public static class CNSMDataFiledNames {

    public static final List<String> OBSERVATION_FIELDS = Arrays.asList(
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

    public static final List<String> CONSEQUENCE_FIELDS = Arrays.asList(
        "gene_affected",
        "transcript_affected",
        "gene_build_version");

    public static final ImmutableMap<String, String> FIELD_MAPPING = ImmutableMap.<String, String> builder()
        .put("_donor_id", "icgc_donor_id")
        .put("_project_id", "project_code")
        .put("_specimen_id", "icgc_specimen_id")
        .put("_sample_id", "icgc_sample_id")
        .put("_matched_sample_id", "matched_icgc_sample_id")
        .put("analyzed_sample_id", "submitted_sample_id")
        .put("matched_sample_id", "submitted_matched_sample_id")
        .build();

    public static final List<String> ALL_FIELDS = Lists.newArrayList(
        Iterables.concat(OBSERVATION_FIELDS, CONSEQUENCE_FIELDS, FIELD_MAPPING.values()));

  }

}
