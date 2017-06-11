package org.icgc.dcc.release.job.id.test.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.id.client.core.IdClientFactory;
import org.icgc.dcc.release.job.id.test.function.mock.MockIdClientFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by gguo on 6/10/17.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        AddSurrogateDonorIdTest.class,
        AddSurrogateMutaionIdTest.class,
        AddSurrogateSampleIdTest.class,
        AddSurrogateSpecimenIdTest.class,
        ExportStringParserTest.class
})
public class IdJobTestSuite {

    public static IdClientFactory factory;
    public static JavaSparkContext sc;
    public static SparkConf conf;
    @BeforeClass
    public static void initialize(){
        factory = new MockIdClientFactory("", "");
        conf = (new SparkConf()).setMaster("local[4]").setAppName("AddSurrogateDonorIdTest");
        sc = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void tearDown(){
        sc.close();
    }
}
