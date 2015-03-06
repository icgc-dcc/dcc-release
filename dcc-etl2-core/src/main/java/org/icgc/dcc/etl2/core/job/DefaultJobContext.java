package org.icgc.dcc.etl2.core.job;

import java.util.List;

import lombok.Value;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Table;

@Value
public class DefaultJobContext implements JobContext {

  String releaseName;
  List<String> projectNames;

  String releaseDir;
  String workingDir;

  Table<String, String, List<Path>> files;

}
