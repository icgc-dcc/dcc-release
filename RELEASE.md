ICGC DCC - Running Release
===

The steps required to prepare and run the DCC Release pipeline. 


### 1. Preparing for run.

Install the latest Oracle Java 8 runtime environment.

Download the latest version of the `dcc-release-client` jar from the Artifactory:

```
https://artifacts.oicr.on.ca/artifactory/simple/dcc-release/org/icgc/dcc/dcc-release-client/<version>/dcc-release-client-<version>.jar
```

Update client's configuration with correct values:

```
https://github.com/icgc-dcc/dcc-release/blob/develop/dcc-release-client/src/main/resources/application.yml
```

Install/update reference database using the [DCC Importer](https://github.com/icgc-dcc/dcc-import) which prepares a MongoDB database containing projects, go, genes, CGC and pathways data.

(Optional) For a better performance install [Snappy libraries](https://github.com/google/snappy) on the ETL execution environment.

### 2. Run

Execute the following command in the command line:

```
  java \
    -Djava.library.path=${DRIVER_LIB} \
    -Dlogging.config=logback.xml \
    -jar $JAR \
    --spring.config.location=application.yml \
    --release-dir ${DATA_DIR} \
    --staging-dir ${WORK_DIR} \
    --release ${RELEASE_NAME} \
    --project-names ${PROJ} \
    --jobs ${JOBS}
```
where

 - `${DRIVER_LIB}` - path to the native libraries(E.g. snappy).
 -  [logback.xml](https://github.com/icgc-dcc/dcc-release/blob/develop/dcc-release-client/src/main/conf/logback.xml) - logging configuration.
 -  [application.yml](https://github.com/icgc-dcc/dcc-release/blob/develop/dcc-release-client/src/main/resources/application.yml) - `dcc-release-client` configuration file.
 -  `${DATA_DIR}` - path to the clinical data to be used for the ETL process.
 -  `${WORK_DIR}` - work directory on HDFS used to store output files created after each release job is run.
 -  `${PROJ}` - for what projects the ETL process should be performed.
 -  `${JOBS}` - jobs to be run. E.g. `STAGE`,`MASK`.
