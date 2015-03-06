ICGC DCC - ETL2 Workflow
===

Entry point to the ETL process.

Build
---

From the command line:

`cd dcc-etl`

`mvn clean package -DskipTests -am -pl :dcc-etl2-workflow`

Run
---

From the command line:

`java -jar dcc-etl-workflow-[VERSION].jar --spring.profiles.active=[development|production]`
