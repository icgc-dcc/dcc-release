/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.release.core.util;

import static org.icgc.dcc.common.core.model.FieldNames.GENE_SET_NAME;

public class FieldNames {

  public static class OrphanFieldNames {

    public static final String ORPHANED_FIELD = "orphaned";

  }

  public static class JoinFieldNames {

    public static final String EXPRESSED_ALLELE = "expressed_allele";
    public static final String QUALITY_SCORE = "quality_score";
    public static final String PROBABILITY = "probability";
    public static final String BIOLOGICAL_VALIDATION_PLATFORM = "biological_validation_platform";
    public static final String EXPERIMENTAL_PROTOCOL = "experimental_protocol";
    public static final String THERAPY = "therapy";
    public static final String FAMILY = "family";
    public static final String EXPOSURE = "exposure";
    public static final String BIOMARKER = "biomarker";
    public static final String SURGERY = "surgery";
    public static final String JUNCTION_SEQ = "junction_seq";
    public static final String SEQ_COVERAGE = "seq_coverage";
    public static final String SECOND_GENE_STABLE_ID = "second_gene_stable_id";
    public static final String OTHER_ANALYSIS_ALGORITHM = "other_analysis_algorithm";
    public static final String JUNCTION_TYPE = "junction_type";
    public static final String GENE_STABLE_ID = "gene_stable_id";
    public static final String PROBE_ID = "probe_id";
    public static final String ARRAY_PLATFORM = "array_platform";
    public static final String MUTATION_ID = "mutation_id";
    public static final String SV_ID = "sv_id";
    public static final String PLACEMENT = "placement";
    public static final String ALIGNMENT_ALGORITHM = "alignment_algorithm";
    public static final String BASE_CALLING_ALGORITHM = "base_calling_algorithm";
    public static final String BIOLOGICAL_VALIDATION_STATUS = "biological_validation_status";
    public static final String MUTANT_ALLELE_READ_COUNT = "mutant_allele_read_count";
    public static final String TOTAL_READ_COUNT = "total_read_count";
    public static final String VARIATION_CALLING_ALGORITHM = "variation_calling_algorithm";

  }

  public static class SummarizeFieldNames {

    public static final String GENE_COUNT = "_gene_count";
    public static final String GENE_NAME = GENE_SET_NAME;
    public static final String FAKE_GENE_ID = "";

  }

  public static class IndexFieldNames {

    public static final String GO_TERM_ONTOLOGY = "ontology";
    public static final String TEXT_TYPE_ID = "id";

  }

  public static class ExportFieldNames {

    public static final String STUDY_SPECIMEN_INVOLVED_IN = "study_specimen_involved_in";
    public static final String STUDY_DONOR_INVOLVED_IN = "study_donor_involved_in";

  }

  public static class DocumentFieldNames {

    public static final String GENE_SET_PATHWAY = "gene_set_pathway";

  }

}
