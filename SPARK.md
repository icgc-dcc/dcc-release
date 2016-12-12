# Spark

DCC Release application uses [Apache Spark](http://spark.apache.org) engine to execute different steps of data processing.

### Build

To run Spark 1.6.1 on CDH 5.3.1 using Hadoop MR1 (DCC requirement), a custom build is required since http://spark.apache.org/downloads.html does not offer a pre-built distribution of CDH5:

```bash
git clone https://github.com/apache/spark.git
cd spark
git checkout v1.6.1
./make-distribution.sh --tgz --skip-java-test -DskipTests -Phadoop-2.4 -Dhadoop.version=2.5.0-mr1-cdh5.3.1
stat spark-1.6.1-bin-2.5.0-mr1-cdh5.3.1.tgz
```

It may be necessary to skip build of `examples` module. Otherwise compilation might fail. The following diff shows what modifications were made to the `make-distribution.sh` file in order to build a Spark engine with the custom configuration:

```bash
diff --git a/make-distribution.sh b/make-distribution.sh
index dd990d4..e5e0d34 100755
--- a/make-distribution.sh
+++ b/make-distribution.sh
@@ -195,14 +195,14 @@ echo "Build flags: $@" >> "$DISTDIR/RELEASE"

 # Copy jars
 cp "$SPARK_HOME"/assembly/target/scala*/*assembly*hadoop*.jar "$DISTDIR/lib/"
-cp "$SPARK_HOME"/examples/target/scala*/spark-examples*.jar "$DISTDIR/lib/"
+#cp "$SPARK_HOME"/examples/target/scala*/spark-examples*.jar "$DISTDIR/lib/"
 # This will fail if the -Pyarn profile is not provided
 # In this case, silence the error and ignore the return code of this command
 cp "$SPARK_HOME"/network/yarn/target/scala*/spark-*-yarn-shuffle.jar "$DISTDIR/lib/" &> /dev/null || :

 # Copy example sources (needed for python and SQL)
-mkdir -p "$DISTDIR/examples/src/main"
-cp -r "$SPARK_HOME"/examples/src/main "$DISTDIR/examples/src/"
+#mkdir -p "$DISTDIR/examples/src/main"
+#cp -r "$SPARK_HOME"/examples/src/main "$DISTDIR/examples/src/"

 if [ "$SPARK_HIVE" == "1" ]; then
   cp "$SPARK_HOME"/lib_managed/jars/datanucleus*.jar "$DISTDIR/lib/"
diff --git a/pom.xml b/pom.xml
index 09cdf36..b62d3f9 100644
--- a/pom.xml
+++ b/pom.xml
@@ -103,7 +103,7 @@
     <module>external/flume-sink</module>
     <module>external/mqtt</module>
     <module>external/zeromq</module>
-    <module>examples</module>
+    <!-- module>examples</module -->
     <module>repl</module>
   </modules>
```

See [Specifying the Hadoop Version](http://spark.apache.org/docs/1.6.1/building-spark.html#specifying-the-hadoop-version) Spark documentation for details.


### Configuration

The following configuration is valid for DCC production cluster.

#### `conf/spark-defaults.conf`

Point to installation of Hadoop native libraries:

```bash
spark.executor.extraClassPath   /opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/hadoop-lzo.jar
spark.executor.extraLibraryPath /opt/cloudera/parcels/CDH/lib/hadoop/lib/native:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native
spark.driver.extraLibraryPath   /opt/cloudera/parcels/CDH/lib/hadoop/lib/native:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native
```

#### `conf/spark-env.sh`

Point to installation of Hadoop native libraries and configure Spark cluster according to the available resources:

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
