ICGC DCC - ETL2
===

Second generation of the ICGC DCC ETL build on Spark

Build
---

From the command line:

	mvn package
	
Spark
---

To run with Hadoop MR1 on CDH 5.3.1 with Spark, a custom build is required since http://spark.apache.org/downloads.html does not support the latest CDH:

- `git clone https://github.com/apache/spark.git`
- `cd spark`
- `git checkout v1.2.1`
- `./make-distribution.sh --tgz --skip-java-test -DskipTests -Dhadoop.version=2.5.0-mr1-cdh5.3.1`
- `ls dist`

See http://spark.apache.org/docs/1.2.1/building-spark.html#specifying-the-hadoop-version for details.