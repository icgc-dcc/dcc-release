package org.icgc.dcc.etl2.job.export.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class Constants {

  public static final String DONOR_ID_ = "donor_id";
  public static final String ICGC_DONOR_ID = "icgc_donor_id";
  public static final String ICGC_DONOR_ID_PREFIX = "DO";

  public static final String SPECIMEN_FIELD_NAME = "specimen";
  public static final String EMPTY_SPECIMEN_VALUE =
      "[\"_specimen_id\":\"\",\"specimen_id\":\"\",\"specimen_type\":\"\",\"specimen_type_other\":\"\",\"specimen_interval\":\"\",\"specimen_donor_treatment_type\":\"\",\"specimen_donor_treatment_type_other\":\"\",\"specimen_processing\":\"\",\"specimen_processing_other\":\"\",\"specimen_storage\":\"\",\"specimen_storage_other\":\"\",\"tumour_confirmed\":\"\",\"specimen_biobank\":\"\",\"specimen_biobank_id\":\"\",\"specimen_available\":\"\",\"tumour_histological_type\":\"\",\"tumour_grading_system\":\"\",\"tumour_grade\":\"\",\"tumour_grade_supplemental\":\"\",\"tumour_stage_system\":\"\",\"tumour_stage\":\"\",\"tumour_stage_supplemental\":\"\",\"digital_image_of_stained_section\":\"\"]";

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
}
