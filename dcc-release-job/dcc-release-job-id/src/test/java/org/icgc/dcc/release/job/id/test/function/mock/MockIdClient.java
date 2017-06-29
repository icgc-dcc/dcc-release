package org.icgc.dcc.release.job.id.test.function.mock;

import com.google.common.base.Joiner;
import org.icgc.dcc.id.client.core.IdClient;
import org.icgc.dcc.release.job.id.model.DonorID;
import org.icgc.dcc.release.job.id.model.MutationID;
import org.icgc.dcc.release.job.id.model.SampleID;
import org.icgc.dcc.release.job.id.model.SpecimenID;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by gguo on 6/9/17.
 */
public class MockIdClient implements IdClient {

    private int next_id_donor = 100;
    private int next_id_sample = 100;
    private int next_id_specimen = 100;
    private int next_id_mutation = 100;


    @Override
    public Optional<String> getDonorId(String submittedDonorId, String submittedProjectId) {
        String id = MockCaches.getInstance().getDonors().get(new DonorID(submittedDonorId, submittedProjectId));
        return (id == null)?Optional.empty():Optional.of(id);
    }

    @Override
    public Optional<String> getSampleId(String submittedSampleId, String submittedProjectId) {
        String id = MockCaches.getInstance().getSamples().get(new SampleID(submittedSampleId, submittedProjectId));
        return (id == null)?Optional.empty():Optional.of(id);
    }

    @Override
    public Optional<String> getSpecimenId(String submittedSpecimenId, String submittedProjectId) {
        String id = MockCaches.getInstance().getSpecimens().get(new SpecimenID(submittedSpecimenId, submittedProjectId));
        return (id == null)?Optional.empty():Optional.of(id);
    }

    @Override
    public Optional<String> getMutationId(String chromosome, String chromosomeStart, String chromosomeEnd, String mutation, String mutationType, String assemblyVersion) {
        String id = MockCaches.getInstance().getMutations().get(new MutationID(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion, ""));
        return (id == null)?Optional.empty():Optional.of(id);
}

    @Override
    public Optional<String> getFileId(String submittedFileId) {
        return null;
    }

    @Override
    public String createDonorId(String submittedDonorId, String submittedProjectId) {
        String ret = "";
        synchronized (MockCaches.getInstance().getDonors()) {
            ret = Integer.toString(next_id_donor);
            MockCaches.getInstance().getDonors().put(new DonorID(submittedDonorId + "_" + next_id_donor, submittedProjectId + "_" + next_id_donor), ret);
            next_id_donor++;
        }
        return ret;
    }

    @Override
    public String createSpecimenId(String submittedSpecimenId, String submittedProjectId) {
        String ret = "";
        synchronized (MockCaches.getInstance().getSpecimens()) {
            ret = Integer.toString(next_id_specimen);
            MockCaches.getInstance().getSpecimens().put(new SpecimenID(submittedSpecimenId + "_" + next_id_specimen, submittedProjectId + "_" + next_id_specimen), ret);
            next_id_specimen++;
        }
        return ret;
    }

    @Override
    public String createSampleId(String submittedSampleId, String submittedProjectId) {
        String ret = "";
        synchronized (MockCaches.getInstance().getSamples()) {
            ret = Integer.toString(next_id_sample);
            MockCaches.getInstance().getSamples().put(new SampleID(submittedSampleId + "_" + next_id_sample, submittedProjectId + "_" + next_id_sample), ret);
            next_id_sample++;
        }
        return ret;
    }

    @Override
    public String createMutationId(String chromosome, String chromosomeStart, String chromosomeEnd, String mutation, String mutationType, String assemblyVersion) {
        String ret = "";
        synchronized (MockCaches.getInstance().getMutations()) {
            ret = Integer.toString(next_id_mutation);
            MockCaches.getInstance().getMutations().put(new MutationID(chromosome + "_" + next_id_mutation, chromosomeStart + "_" + next_id_mutation, chromosomeEnd+"_"+next_id_mutation, mutation+"_"+next_id_mutation, mutationType+"_"+next_id_mutation, "GRCh37", ""), ret);
            next_id_mutation++;
        }
        return ret;
    }

    @Override
    public String createFileId(String submittedFileId) {
        return null;
    }

    @Override
    public Optional<String> getAllDonorIds() {

        return
                Optional.of(
                    Joiner.on("\n").join(
                        MockCaches.getInstance().getDonors().entrySet().stream().map(entry -> {
                            DonorID doner = entry.getKey();
                            String id = entry.getValue();
                            return Joiner.on('\t').join(id, doner.getId(), doner.getProject());
                        }).collect(Collectors.toList()).toArray()
                    )
                );
    }

    @Override
    public Optional<String> getAllSampleIds() {
        return
                Optional.of(
                        Joiner.on("\n").join(
                                MockCaches.getInstance().getSamples().entrySet().stream().map(entry -> {
                                    SampleID sample = entry.getKey();
                                    String id = entry.getValue();
                                    return Joiner.on('\t').join(id, sample.getId(), sample.getProject());
                                }).collect(Collectors.toList()).toArray()
                        )
                );
    }

    @Override
    public Optional<String> getAllSpecimenIds() {
        return
                Optional.of(
                        Joiner.on("\n").join(
                                MockCaches.getInstance().getSpecimens().entrySet().stream().map(entry -> {
                                    SpecimenID specimen = entry.getKey();
                                    String id = entry.getValue();
                                    return Joiner.on('\t').join(id, specimen.getId(), specimen.getProject());
                                }).collect(Collectors.toList()).toArray()
                        )
                );
    }

    @Override
    public Optional<String> getAllMutationIds() {
        return
                Optional.of(
                        Joiner.on("\n").join(
                                MockCaches.getInstance().getMutations().entrySet().stream().map(entry -> {
                                    MutationID mutation = entry.getKey();
                                    String id = entry.getValue();
                                    return Joiner.on('\t').join(id, mutation.getChromosome(), mutation.getChromosomeStart(), mutation.getChromosomeEnd(), mutation.getMutation(), mutation.getMutationType(), mutation.getAssemblyVersion());
                                }).collect(Collectors.toList()).toArray()
                        )
                );
    }

    @Override
    public void close() throws IOException {

    }

    public int getNext_id_donor() {
        return next_id_donor;
    }

    public int getNext_id_sample() {
        return next_id_sample;
    }

    public int getNext_id_specimen() {
        return next_id_specimen;
    }

    public int getNext_id_mutation() {
        return next_id_mutation;
    }
}
