# SnpEff database upgrade

A snpEff database upgrade is required when:

- a snpeff tool is upgraded
- reference genome is upgraded
- gene annotations are upgraded

## Building a SnpEff prediction database
These instructions will illustrate a process of building a SnpEff prediction database on a Mac computer for the latest(4.2 at the moment of writing this documentation) version of `SnpEff`.  The database will be built using `GTF` files which is the easiest way according [to the documentation](http://snpeff.sourceforge.net/SnpEff_manual.html#databases).

`GRCh-37.75` reference genome `FASTA` file and `release-85` `Ensembl` gene annotation files will be used.



1. Create working directory

	```
	mkdir snpeff-4.2 && cd snpeff-4.2
	```

2. Download snpEff from [Sourceforge](http://snpeff.sourceforge.net/download.html)
3. Uncompress

	```
	unzip snpEff_latest_core.zip
	```
	
4. Add information about the database we are going to build to the `snpEff/snpEff.config` file.

	```
	# DCC
	4.2-GRCh37.85.genome : Homo_sapiens
	4.2-GRCh37.85.reference : ftp://ftp.ensembl.org/pub/grch37/release-85/gtf/
	4.2-GRCh37.85.MT.codonTable :  Vertebrate_Mitochondrial
	```
	
5. Create working directory where genome and gene annotation files will be placed

	```
	mkdir -p snpEff/data/4.2-GRCh37.85 && cd snpEff/data/4.2-GRCh37.85
	```
	
6. Download reference genome FASTA file and name it `sequences.fa` (snpEff requirement)

	```
	wget https://artifacts.oicr.on.ca/artifactory/dcc-dependencies/org/icgc/dcc/dcc-reference-genome/GRCh37.75.v1/dcc-reference-genome-GRCh37.75.v1.tar.gz
	tar xf dcc-reference-genome-GRCh37.75.v1.tar.gz
	ln -s GRCh37.75.v1.fasta sequences.fa
	```
	
7. Download gene annotation files. Remove rows in the GTF file (first column is chromosome) that are not for one of these chromosomes: 1 - 22, X, Y, MT.

	```
	wget ftp://ftp.ensembl.org/pub/grch37/release-85/gtf/homo_sapiens/Homo_sapiens.GRCh37.85.gtf.gz
	gzip -d Homo_sapiens.GRCh37.85.gtf.gz
	<remove rows>
	```
	
8. Rename the gene annotations file to `genes.gtf` (snpEff requirement)

	```
	mv Homo_sapiens.GRCh37.85.gtf genes.gtf
	```
	
9. Build a snpEff prediction database

	```
	cd ../..
	java -jar snpEff.jar build -gtf22 -v 4.2-GRCh37.85
	```
	
This will produce a file `data/4.2-GRCh37.85/snpEffectPredictor.bin`.
