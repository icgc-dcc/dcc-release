package org.icgc.dcc.etl2.test.job;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.of;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.List;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.job.DefaultJobContext;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
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
import com.google.common.collect.Table;
import com.google.common.util.concurrent.MoreExecutors;

public abstract class AbstractJobTest {

  /**
   * Constants.
   */
  private static final File TEST_FIXTURES_DIR = new File("src/test/resources/fixtures");

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

  protected JobContext createContext(JobType type) {
    return createContext(type, "");
  }

  @SuppressWarnings("unchecked")
  protected JobContext createContext(JobType type, String projectName) {
    return new DefaultJobContext(type, "ICGC<version>", of(projectName), "/dev/null", workingDir.toString(),
        mock(Table.class));
  }

  protected void createInputFile(TestFile inputFile) {
    val fileTypeDirectory = getFileTypeDirectory(inputFile.getFileType());
    checkState(fileTypeDirectory.mkdirs());

    val target = inputFile.isProjectPartitioned() ?
        getProjectFileTypeDirectory(inputFile.getProjectName(), inputFile.getFileType()) :
        getFileTypeFile(inputFile.getFileType());

    if (inputFile.isFile()) {
      val sourceFile = new File(TEST_FIXTURES_DIR + "/" + inputFile.getFileName());
      TestFiles.writeInputFile(sourceFile, target);
    } else {
      TestFiles.writeInputFile(inputFile.getRows(), target);
    }
  }

  protected TaskExecutor createTaskExecutor() {
    return new TaskExecutor(MoreExecutors.sameThreadExecutor(), sparkContext, fileSystem);
  }

  private File getFileTypeFile(String fileType) {
    return new File(getFileTypeDirectory(fileType), "part-00000");
  }

  private File getFileTypeDirectory(String fileType) {
    val type = new File(workingDir, fileType);

    return type;
  }

  private File getProjectFileTypeDirectory(String projectName, String fileType) {
    return new File(getFileTypeDirectory(fileType), Partitions.getPartitionName(projectName));
  }

  private File getProjectFileTypeFile(String projectName, String fileType) {
    return new File(getProjectFileTypeDirectory(projectName, fileType), "part-00000");
  }

  @SneakyThrows
  protected List<ObjectNode> produces(String projectName, String fileType) {
    val file = projectName == null ? getFileTypeFile(fileType) : getProjectFileTypeFile(projectName, fileType);

    return TestFiles.readInputFile(file);
  }

  @SneakyThrows
  protected List<ObjectNode> produces(String fileType) {
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

}
