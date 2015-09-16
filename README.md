ICGC DCC - Release
===

Second generation of the ICGC DCC ETL build on Spark

Build
---

From the command line:

```bash
mvn clean package
```
	
Spark
---

### Build

To run Spark 1.2.1 on CDH 5.3.1 using Hadoop MR1 (DCC requirement), a custom build is required since http://spark.apache.org/downloads.html does not offer a pre-built distribution of CDH5:

```bash
git clone https://github.com/apache/spark.git
cd spark
git checkout v1.2.1
./make-distribution.sh --tgz --skip-java-test -DskipTests -Phadoop-2.4 -Dhadoop.version=2.5.0-mr1-cdh5.3.1
stat spark-1.2.1-bin-2.5.0-mr1-cdh5.3.1.tgz
```

See http://spark.apache.org/docs/1.2.1/building-spark.html#specifying-the-hadoop-version for details.


### Configuration

#### `conf/spark-defaults.conf`

```bash
spark.executor.extraClassPath   /opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/hadoop-lzo.jar
spark.executor.extraLibraryPath /opt/cloudera/parcels/CDH/lib/hadoop/lib/native:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native
spark.driver.extraLibraryPath   /opt/cloudera/parcels/CDH/lib/hadoop/lib/native:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native
```

#### `conf/spark-env.sh`

```bash
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native:/opt/cloudera/parcels/CDH/lib/hadoop/lib/native
export JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native:/opt/cloudera/parcels/CDH/lib/hadoop/lib/native
export SPARK_CLASSPATH=$SPARK_CLASSPATH:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/hadoop-lzo.jar
export SPARK_PRINT_LAUNCH_COMMAND=1

export SPARK_MASTER_IP="<master-host-name>"
export SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=1"
export SPARK_WORKER_CORES=6
export SPARK_WORKER_MEMORY=32g
export SPARK_WORKER_INSTANCES=4
```

#### `conf/slaves`

Add all slaves hostnames to this file
