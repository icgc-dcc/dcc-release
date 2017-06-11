package org.icgc.dcc.release.job.id.model;

import com.sun.istack.NotNull;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by gguo on 6/8/17.
 */
@Data
public class SampleID implements Serializable {
    @NotNull
    private final String id;

    @NotNull
    private final String project;
}
