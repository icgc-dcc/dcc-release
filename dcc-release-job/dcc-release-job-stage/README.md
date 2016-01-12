ICGC DCC - Release Stage Job
===

Stage module for loading files from the submission system into a canonical ETL format. 

The job implementation sses [splittablegzip](https://github.com/nielsbasjes/splittablegzip) to speed up initial load due to large gzipped input files from the submission system.

Build
---

From the command line:

	mvn package

