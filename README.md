# ICGC DCC - Release

Second generation of the ICGC DCC ETL build on Spark. For the first generation ETL project, please see the [dcc-etl](https://github.com/icgc-dcc/dcc-etl) repository.

## Build

To build the application execute the following from the command line:

```bash
mvn clean package
```

## Modules

For a high-level overview of the application please see [PROCESS.md](PROCESS.md).

Sub-system modules:

- [Stage Job](dcc-release-job/dcc-release-job-stage/README.md)
- [Mask Job](dcc-release-job/dcc-release-job-mask/README.md)
- [ID Job](dcc-release-job/dcc-release-job-id/README.md)
- [Image Job](dcc-release-job/dcc-release-job-image/README.md)
- [Annotate Job](dcc-release-job/dcc-release-job-annotate/README.md)
- [Join Job](dcc-release-job/dcc-release-job-join/README.md)
- [Import Job](dcc-release-job/dcc-release-job-import/README.md)
- [FATHMM Job](dcc-release-job/dcc-release-job-fathmm/README.md)
- [Functional Impact Job](dcc-release-job/dcc-release-job-fi/README.md)
- [Summarize Job](dcc-release-job/dcc-release-job-summarize/README.md)
- [Document Job](dcc-release-job/dcc-release-job-document/README.md)
- [Index Job](dcc-release-job/dcc-release-job-index/README.md)
- [Export Job](dcc-release-job/dcc-release-job-export/README.md)

## Spark

For information how to build a custom version of Spark please see [SPARK.md](SPARK.md).

## Running Application

For general instructions how to run a data processing with the dcc-release application please see [RELEASE.md](RELEASE.md).

## DCC Instructions

For DCC specific instructions please see [internal documentation](https://wiki.oicr.on.ca/display/DCCSOFT/Vitalii%27s+transition#Vitalii'stransition-DCCRelease).

## FATHMM

For information on FATHMM, please see [FATHMM.md](FATHMM.md).
