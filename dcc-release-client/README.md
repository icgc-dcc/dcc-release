ICGC DCC - Release Client
===

Entry point to the Release process.

Build
---

From the command line:

`cd dcc-release`

`mvn clean package -DskipTests -am -pl :dcc-release-client`

Run
---

From the command line:

`java -jar dcc-release-client-[VERSION].jar --spring.profiles.active=[development|production]`
