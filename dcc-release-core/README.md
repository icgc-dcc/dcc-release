ICGC DCC - Release Core
===

Core module

Build
---

From the command line:

	mvn package

Utilities
---

#### SmileSequenceFileDecompressor

###### Overview

`SmileSequenceFileDecompressor` uncompresses `Hadoop` `Sequence` files compressed with `Snappy` codec to the plain text format.

###### Setup

Because of how `DCC Release` is built is not possible to run the utility directly. Couple preparation steps are required:

* Uncompress `dcc-release-client` 'uber' jar `jar xvf dcc-release-client-x.x.x.jar`
* Use the following script to run the utility.

```bash
#!/bin/bash -e
VERSION=3.8.6
CLASSPATH="dcc-release-core-${VERSION}.jar:dcc-release-job-all-${VERSION}.jar:hadoop-common-2.5.0-cdh5.3.1.jar:commons-configuration-1.6.jar:hadoop-auth-2.5.0-cdh5.3.1.jar:avro-1.7.7.jar"
cd lib
java \
 -Djava.library.path=/usr/lib/hadoop/lib/native \
 -cp $CLASSPATH \
 org.icgc.dcc.release.core.util.SmileSequenceFileDecompressor $1
```

###### Run

Assuming the script was named `decompress.sh` from the shell execute

```bash
./decompress.sh <file_to_decompress>
```

The utility will write results to file `/tmp/<file_to_decompress>.out`
