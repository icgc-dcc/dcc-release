package org.icgc.dcc.etl2.test.job;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.List;
import java.util.Optional;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.job.DefaultJobContext;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.core.task.DefaultTaskContext;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.core.task.TaskExecutor;
import org.icgc.dcc.etl2.core.util.Partitions;
import org.icgc.dcc.etl2.test.model.TestFile;
import org.icgc.dcc.etl2.test.model.TestFile.TestFileBuilder;
import org.icgc.dcc.etl2.test.util.TestFiles;
import org.icgc.dcc.etl2.test.util.TestJsonNodes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.MoreExecutors;

public abstract class AbstractJobTest {

  /**
   * Constants.
   */
  protected static final String TEST_FIXTURES_DIR = "src/test/resources/fixtures";
  private static final String RELEASE_VERSION = "ICGC19-0-2";

  /**
   * Collaborators.
   */
  protected JavaSparkContext sparkContext;
  protected TaskExecutor taskExecutor;
  protected FileSystem fileSystem;
  protected File workingDir;

  /**
   * State.
   */
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Before
  @SneakyThrows
  public void setUp() {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", "org.icgc.dcc.etl2.core.util.CustomKryoRegistrator");
    sparkConf.set("spark.task.maxFailures", "0");

    this.sparkContext = new JavaSparkContext(sparkConf);

    val executor = MoreExecutors.sameThreadExecutor();
    this.fileSystem = FileSystem.getLocal(new Configuration());
    this.taskExecutor = new TaskExecutor(executor, sparkContext, fileSystem);

    this.workingDir = tmp.newFolder("working");
  }

  @After
  public void shutDown() {
    sparkContext.stop();
    sparkContext = null;
    System.clearProperty("spark.master.port");
  }

  protected void given(TestFileBuilder... inputFileBuilders) {
    for (val inputFileBuilder : inputFileBuilders) {
      createInputFile(inputFileBuilder.build());
    }
  }

  protected void given(File inputDirectory) {
    File[] fileTypes = inputDirectory.listFiles();
    processFileTypes(fileTypes);
  }

  private void processFileTypes(File[] fileTypes) {
    for (File fileTypeDir : fileTypes) {
      if (fileTypeDir.isFile()) {
        continue;
      }
      String fileTypeDirName = fileTypeDir.getName();
      File[] projects = fileTypeDir.listFiles();
      if (areProjects(projects)) {
        processProjects(fileTypeDirName, projects);
      } else {
        processFiles(fileTypeDirName, projects);
      }
    }
  }

  private void processFiles(String fileTypeDirName, File[] projects) {
    for (val project : projects) {
      val testFile = inputFile()
          .fileType(FileType.valueOf(fileTypeDirName.toUpperCase()))
          .path(project.getAbsolutePath())
          .build();
      createInputFile(testFile);
    }
  }

  private boolean areProjects(File[] projects) {
    val projectsList = ImmutableList.copyOf(projects);
    if (projectsList.stream().allMatch(f -> f.getName().startsWith("project_name"))) {
      return true;
    } else if (projectsList.stream().allMatch(f -> isPartFile(f))) {
      return false;
    }

    throw new IllegalArgumentException();
  }

  private void processProjects(String fileTypeDirName, File[] projects) {
    for (File projectDir : projects) {
      if (projectDir.isFile()) {
        continue;
      }
      String projectName = projectDir.getName().split("=")[1];
      File[] files = projectDir.listFiles();
      createInputFiles(files, projectName, fileTypeDirName);
    }
  }

  private void createInputFiles(File[] files, String projectName, String fileTypeDirName) {
    for (File file : files) {
      String fileName = file.getName();
      if (fileName.startsWith("part-")) {
        TestFile testFile =
            TestFile.builder().projectName(projectName).fileType(FileType.valueOf(fileTypeDirName.toUpperCase()))
                .fileName(fileName)
                .path(file.getAbsolutePath()).build();
        createInputFile(testFile);
      }
    }
  }

  protected JobContext createJobContext(JobType type) {
    return createJobContext(type, ImmutableList.of(""));
  }

  @SuppressWarnings("unchecked")
  protected JobContext createJobContext(JobType type, List<String> projectNames) {
    return new DefaultJobContext(type, RELEASE_VERSION, projectNames, "/dev/null",
        workingDir.toString(), mock(Table.class), taskExecutor);
  }

  protected TaskContext createTaskContext(JobType jobType) {
    return new DefaultTaskContext(createJobContext(jobType), sparkContext, fileSystem, Optional.empty());
  }

  protected void createInputFile(TestFile inputFile) {
    val fileTypeDirectory = getFileTypeDirectory(inputFile.getFileType());
    if (!fileTypeDirectory.exists()) {
      fileTypeDirectory.mkdirs();
    }

    val target = inputFile.isProjectPartitioned() ?
        getProjectFileTypeDirectory(inputFile.getProjectName(), inputFile.getFileType()) :
        getFileTypeFile(inputFile.getFileType());
    if (!isPartFile(target) && !target.exists()) {
      target.mkdirs();
    }

    if (inputFile.isFile()) {
      val sourceFile = new File(inputFile.getPath());
      val targetFile = !inputFile.hasFileName() || isPartFile(target) ?
          target : new File(target, sourceFile.getName());
      TestFiles.writeInputFile(sourceFile, targetFile);
    } else {
      val targetFile = new File(target, "part-00000");
      TestFiles.writeInputFile(inputFile.getRows(), targetFile);
    }
  }

  private static boolean isPartFile(File target) {
    return target.getName().startsWith("part-");
  }

  protected TaskExecutor createTaskExecutor() {
    return new TaskExecutor(MoreExecutors.sameThreadExecutor(), sparkContext, fileSystem);
  }

  private File getFileTypeFile(FileType fileType) {
    return new File(getFileTypeDirectory(fileType), "part-00000");
  }

  private File getFileTypeDirectory(FileType fileType) {
    val type = new File(workingDir, fileType.getDirName());

    return type;
  }

  private File getProjectFileTypeDirectory(String projectName, FileType fileType) {
    return new File(getFileTypeDirectory(fileType), Partitions.getPartitionName(projectName));
  }

  private File getProjectFileTypeFile(String projectName, FileType fileType) {
    return new File(getProjectFileTypeDirectory(projectName, fileType), "part-00000");
  }

  protected List<ObjectNode> producesFile(FileType fileType) {
    return producesFile(null, fileType);
  }

  @SneakyThrows
  protected List<ObjectNode> producesFile(String projectName, FileType fileType) {
    val file = projectName == null ? getFileTypeFile(fileType) : getProjectFileTypeFile(projectName, fileType);

    return TestFiles.readInputFile(file);
  }

  @SneakyThrows
  protected List<ObjectNode> produces(String projectName, FileType fileType) {
    val file = projectName == null ?
        getFileTypeDirectory(fileType) :
        getProjectFileTypeDirectory(projectName, fileType);

    return TestFiles.readInputDirectory(file);
  }

  @SneakyThrows
  protected List<ObjectNode> produces(FileType fileType) {
    return produces(null, fileType);
  }

  protected static ObjectNode row(@NonNull String json) {
    return TestJsonNodes.$(json);
  }

  protected static TestFileBuilder inputFile() {
    return TestFile.builder();
  }

  protected static TestFileBuilder inputFile(String projectName) {
    return inputFile().projectName(projectName);
  }

  protected String resolvePath(String fileName) {
    return new File(TEST_FIXTURES_DIR + "/" + fileName).getAbsolutePath();
  }

}
