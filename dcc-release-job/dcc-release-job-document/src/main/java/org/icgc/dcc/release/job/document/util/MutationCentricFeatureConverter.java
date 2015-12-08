/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.document.util;

import static com.google.common.base.Strings.nullToEmpty;

import java.util.List;

import lombok.val;

import org.icgc.dcc.release.job.document.vcf.model.Consequence;
import org.icgc.dcc.release.job.document.vcf.model.Feature;
import org.icgc.dcc.release.job.document.vcf.model.Mutation;
import org.icgc.dcc.release.job.document.vcf.model.MutationType;
import org.icgc.dcc.release.job.document.vcf.model.Occurrence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class MutationCentricFeatureConverter {

  public Feature convert(ObjectNode source) {
    // Build
    List<Occurrence> occurrences = createOccurrences(source);
    List<Consequence> consequences = createConsequences(source);
    Feature feature = createFeature(source, occurrences, consequences);

    return feature;
  }

  private static List<Occurrence> createOccurrences(ObjectNode source) {
    val projectSsmAffectedDonorIds = HashMultimap.<String, String> create();
    val projectSsmTestedDonorCounts = Maps.<String, Integer> newHashMap();

    for (JsonNode ssmOccurrence : source.get("ssm_occurrence")) {
      val projectId = ssmOccurrence.at("/project/_project_id").textValue();
      val projectSsmTestedDonorCount = ssmOccurrence.at("/project/_summary/_ssm_tested_donor_count").intValue();
      val donorId = ssmOccurrence.at("/donor/_donor_id").textValue();

      projectSsmAffectedDonorIds.put(projectId, donorId);
      projectSsmTestedDonorCounts.put(projectId, projectSsmTestedDonorCount);
    }

    val occurrences = ImmutableList.<Occurrence> builder();
    for (val projectCountsEntry : projectSsmTestedDonorCounts.entrySet()) {
      val projectId = projectCountsEntry.getKey();
      val ssmAffectedDonors = projectSsmAffectedDonorIds.get(projectId).size();
      val ssmTestedDonors = projectCountsEntry.getValue();
      val frequency = frequency(ssmAffectedDonors, ssmTestedDonors);

      occurrences.add(new Occurrence()
          .projectCode(projectId)
          .affectedDonors(ssmAffectedDonors)
          .testedDonors(ssmTestedDonors)
          .frequency(frequency));
    }

    return occurrences.build();
  }

  private static List<Consequence> createConsequences(ObjectNode source) {
    val consequences = ImmutableList.<Consequence> builder();
    for (JsonNode transcript : source.get("transcript")) {
      consequences.add(new Consequence()
          .geneSymbol(text(transcript.at("/gene/symbol")))
          .geneAffected(text(transcript.at("/gene/_gene_id")))
          .geneStrand(strand(text(transcript.at("/gene/strand"))))
          .transcriptName(text(transcript.at("/name")))
          .transcriptAffected(text(transcript.at("/id")))
          .proteinAffected(text(transcript.at("/translation_id")))
          .consequenceType(text(transcript.at("/consequence/consequence_type")))
          .cdsMutation(text(transcript.at("/consequence/cds_mutation")))
          .aaMutation(text(transcript.at("/consequence/aa_mutation"))));
    }

    return consequences.build();
  }

  private static Feature createFeature(ObjectNode source, List<Occurrence> occurrences, List<Consequence> consequences) {
    String value = source.get("mutation").textValue();
    String altAllele = value.split(">")[1];
    Mutation mutation = Mutation.builder()
        .mutationType(MutationType.fromText(source.get("mutation_type").textValue()))
        .mutation(value)
        .reference(source.get("reference_genome_allele").textValue())
        .affectedDonors(source.at("/_summary/_affected_donor_count").intValue())
        .testedDonors(source.at("/_summary/_tested_donor_count").intValue())
        .projectCount(source.at("/_summary/_affected_project_count").intValue())
        .occurrences(occurrences)
        .consequences(consequences)
        .build();

    return Feature.builder()
        .id(source.get("_mutation_id").textValue())
        .chromosome(source.get("chromosome").textValue())
        .chromosomeStart(source.get("chromosome_start").longValue())
        .chromosomeEnd(source.get("chromosome_end").longValue())
        .altAllele(altAllele)
        .mutation(mutation)
        .build();
  }

  private static String frequency(int affectedDonors, int testedDonors) {
    return String.format("%.5f", affectedDonors * 1.0f / testedDonors);
  }

  private static String strand(String text) {
    if (text.equals("1")) {
      return "+";
    } else if (text.equals("-1")) {
      return "1";
    } else {
      return "";
    }
  }

  private static String text(JsonNode jsonNode) {
    if (jsonNode.canConvertToInt()) {
      return String.valueOf(jsonNode.asInt());
    }

    return nullToEmpty(jsonNode.textValue());
  }

}
