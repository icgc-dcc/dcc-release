package org.icgc.dcc.release.job.id.model;

import com.sun.istack.NotNull;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by gguo on 6/8/17.
 */
@Data
public class MutationID implements Serializable {

    @NotNull
    private final String chromosome;

    @NotNull
    private final String chromosomeStart;

    @NotNull
    private final String chromosomeEnd;

    @NotNull
    private final String mutation;

    @NotNull
    private final String mutationType;

    @NotNull
    private final String assemblyVersion;

}
