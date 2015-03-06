ICGC DCC - ETL2
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

To run Spark 1.2.1 on CDH 5.3.1 using Hadoop MR1 (DCC requirement), a custom build is required since http://spark.apache.org/downloads.html does not offer a pre-built distribution of CDH5:

```bash
git clone https://github.com/apache/spark.git
cd spark
git checkout v1.2.1
./make-distribution.sh --tgz --skip-java-test -DskipTests -Phadoop-2.4 -Dhadoop.version=2.5.0-mr1-cdh5.3.1
stat spark-1.2.1-bin-2.5.0-mr1-cdh5.3.1.tgz
```

See http://spark.apache.org/docs/1.2.1/building-spark.html#specifying-the-hadoop-version for details.
