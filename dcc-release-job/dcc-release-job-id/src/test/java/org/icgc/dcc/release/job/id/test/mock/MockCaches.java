package org.icgc.dcc.release.job.id.test.mock;

import org.icgc.dcc.release.job.id.model.DonorID;
import org.icgc.dcc.release.job.id.model.MutationID;
import org.icgc.dcc.release.job.id.model.SampleID;
import org.icgc.dcc.release.job.id.model.SpecimenID;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by gguo on 6/9/17.
 */
public class MockCaches {

    private static MockCaches instance = new MockCaches();

    private Map<DonorID, String> donors = null;
    private Map<SampleID, String> samples = null;
    private Map<MutationID, String> mutations = null;
    private Map<SpecimenID, String> specimens = null;

    public static MockCaches getInstance() {
        return instance;
    }

    public Map<DonorID, String> getDonors(){
        return DonorCacheHolder.cache;
    }

    public Map<SampleID, String> getSamples(){
        return SampleCacheHolder.cache;
    }

    public Map<MutationID, String> getMutations(){
        return MutationCacheHolder.cache;
    }

    public Map<SpecimenID, String> getSpecimens(){
        return SpecimenCacheHolder.cache;
    }

    private static class DonorCacheHolder{
        static final Map<DonorID, String> cache = compute();

        private static Map<DonorID, String> compute(){
            Map<DonorID, String> map = new HashMap<>();

            IntStream.range(0, 100).forEach(value -> {
                map.put(new DonorID("DO"+value, "project_"+ value), Integer.toString(value));
            });
            return map;
        }
    }

    private static class SampleCacheHolder{
        static final Map<SampleID, String> cache = compute();

        private static Map<SampleID, String> compute(){
            Map<SampleID, String> map = new HashMap<>();

            IntStream.range(0, 100).forEach(value -> {
                map.put(new SampleID("SA"+value, "project_"+ value), Integer.toString(value));
            });
            return map;
        }
    }

    private static class SpecimenCacheHolder{
        static final Map<SpecimenID, String> cache = compute();

        private static Map<SpecimenID, String> compute(){
            Map<SpecimenID, String> map = new HashMap<>();

            IntStream.range(0, 100).forEach(value -> {
                map.put(new SpecimenID("SP"+value, "project_"+ value), Integer.toString(value));
            });
            return map;
        }
    }

    private static class MutationCacheHolder{
        static final Map<MutationID, String> cache = compute();

        private static Map<MutationID, String> compute(){
            Map<MutationID, String> map = new HashMap<>();

            IntStream.range(0, 100).forEach(value -> {
                map.put(new MutationID("chromosome_"+value, "chromosomeStart_"+ value, "chromosomeEnd_" + value, "mutation_" + value, "mutationType_" + value, "GRCh37", "MU"+value), Integer.toString(value));
            });
            return map;
        }
    }

}
