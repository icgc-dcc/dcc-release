package org.icgc.dcc.release.job.id.model;

import com.sun.istack.NotNull;
import lombok.Data;

/**
 * Created by gguo on 6/8/17.
 */
@Data
public class FileID {
    @NotNull
    private final String id;
}
