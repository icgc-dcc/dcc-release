FATHMM
===
Functional Analysis through Hidden Markov Models (FATHMM) is a precomputed dataset used by ICGC to do functional impact prediction on mutations.



FATHMM
---
FATHMM is a project with its own website which can be found here: http://fathmm.biocompute.org.uk/

The dataset is a database dump and is available here: http://fathmm.biocompute.org.uk/database/fathmm.v2.3.SQL.gz


Purpose
---
FATHMM is used to do functional impact prediction for mutations of missense_variant consequences. We feed the predictor a translation_id and mutation aa_change, and we get a result that is one of {TOLERATED, HIGH, UNKNOWN} as the output.


Data Import
---
Import the dataset into Postgresql database
- Extract the database dump files from the gz archive
- Copy the appropriate dump files to the database server
- Run `psql fathmm < fathmm.v2.3.pSQL`, the dump file will be around 20GB, expect the import to take 3-4 hours.
- The SQL dump may have a few issues, we need to manually patch some of the tables and sequences before we can use FATHMM, see Patch section below.

A backup of FATHMM dump files and the patch can be found under
- /nfs/backups/workspace/fathmm

DCC_CACHE
---
DCC Cache is a cache table that stores {translation_id, aa_change} -> prediction key value pairs. Before we run the actual prediction lookup, we will check if the result already exist in DCC_CACHE first. Please note that when updating to another version of FATHMM dataset, DCC_CACHE needs to be cleared to ensure data correctness.


ICGC Patch
---
The patch creates a DCC_CACHE table and make sure the permissions are correct.

```
create index i1 on "DOMAINS" (id);
create index i2 on "DOMAINS" (hmm);

create table "DCC_CACHE" (
   "translation_id" varchar(64) NOT NULL,
   "aa_mutation" varchar(64) NOT NULL,
   "score" varchar(16),
   "prediction" varchar(16)
);

create index c1 on "DCC_CACHE" (translation_id, aa_mutation);


grant select, insert on "DOMAINS" to dcc;
grant select, insert on "LIBRARY" to dcc;
grant select, insert on "PHENOTYPES" to dcc;
grant select, insert on "PROBABILITIES" to dcc;
grant select, insert on "PROTEIN" to dcc;
grant select, insert on "SEQUENCE" to dcc;
grant select, insert on "VARIANTS" to dcc;
grant select, insert on "WEIGHTS" to dcc;
grant select, insert on "DCC_CACHE" to dcc;
```
