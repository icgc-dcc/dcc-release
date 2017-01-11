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

#### Using a wrapper script

Alternatively, the [release.sh](https://github.com/icgc-dcc/dcc-release/blob/develop/dcc-release-client/src/main/bin/release.sh) script could be used to do a release run. 

The script tracks run history. The feature ensures that each ETL run has a unique name, thus a production Elasticsearch index would not be overridden by accident. It is also possible to configure how resources allocated to jobs. For example, set that the `AnnotateJob` should only be allocated 50 cores.

##### Configuration

The script's configuration relies on the DCC standard application layout:

```shell
$ ls -1
bin
conf
lib
logs
```

The script itself is located in the `bin` directory and all its configuration resides in the `conf` directory. The script has following configuration files: `release-env.sh`, `jobs.properties` and `.lastrun`.

###### release-env.sh
`release-env.sh` contains the default project configuration information. For example, 2 default projects are specified below. If the script is invoked without any project configuration release jobs will use data file for those projects only.

```shell
PROJ="ALL-US,AML-US"
```

###### jobs.properties
`jobs.properties` configures default job resources allocation. For example, the `IndexJob` is allocated only 10 Spark cores, otherwise our production Elasticsearch cluster will be overloaded with index requests.

`jobs.properties` example: 

```shell
ANNOTATE=450
FATHMM=50
INDEX=10
```

###### .lastrun
`.lastrun` contains release name of the last run. For example, `ICGC23-9`. It's not required to create the file manually. It will be created and updated automatically.

##### Running the script

To start a complete processing of ICGC23 release data use the following command:

```shell
$ ./release.sh ICGC23
```

You can always to ask for help to find out about other ways how to run the scirpt.

```shell
$ ./release.sh -h
Usage: release.sh [OPTIONS] RELEASE
  or: release.sh -h
Run DCC RELEASE.
For example: release.sh ICGC21

Options:
  -c    u of cores to use. Defaults to maximum allowed
  -j    Jobs to run. Could be a comma-separated list or a jobs range
  -p    A comma-separated list of projects to run the release for
  -h    Print this message
```

To execute release only couple jobs or resume execution see the following examples.

Execute all jobs including the `IndexJob`:

```shell
$ ./release.sh -j -INDEX ICGC23
```

Execute all jobs staring from the `AnnotateJob`

```shell
$ ./release.sh -j ANNOTATE- ICGC23
```

Execute `AnnotateJob` and `JoinJob`

```shell
$ ./release.sh -j ANNOTATE-JOIN ICGC23
```
