package org.icgc.dcc.etl2.core.submission;

import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

/**
 * Service for interacting with the DCC submission system.
 */
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class SubmissionFileSystem {

  /**
   * Dependencies.
   */
  @NonNull
  private final FileSystem fileSystem;

  @NonNull
  @SneakyThrows
  public Table<String, String, List<Path>> getFiles(String releaseDir, List<String> projectNames, List<Schema> schemas) {
    val watch = createStarted();
    log.info("Resolving submission files...");

    val table = TreeBasedTable.<String, String, List<Path>> create();
    val iterator = fileSystem.listFiles(new Path(releaseDir), true);

    while (iterator.hasNext()) {
      val status = iterator.next();
      val path = status.getPath();

      for (val schema : schemas) {
        val name = path.getName();
        if (name.matches(schema.getPattern())) {
          addFile(projectNames, schema, path, table);
        }
      }
    }

    log.info("Finished resolving submission files in {}", watch);
    return table;
  }

  private void addFile(List<String> projectNames, Schema schema, Path path, Table<String, String, List<Path>> files) {
    val schemaName = schema.getName();
    val projectName = path.getParent().getName();
    if (isTestProject(projectName)) {
      // Skip test projects
      return;
    }

    if (!projectNames.contains(projectName)) {
      // Skip unspecified projects
      return;
    }

    List<Path> paths = files.get(schemaName, projectName);
    if (paths == null) {
      paths = newArrayList();
      files.put(schemaName, projectName, paths);
    }

    paths.add(path);
  }

  private static boolean isTestProject(String projectName) {
    return projectName.startsWith("TEST");
  }

}
