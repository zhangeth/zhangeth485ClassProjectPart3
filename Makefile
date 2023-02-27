JAVAC=javac
JAVA=java
CLASSPATH=.:lib/*
OUTDIR=out

SOURCEDIR=src

sources = $(wildcard $(SOURCEDIR)/**/*.java $(SOURCEDIR)/**/models/*.java $(SOURCEDIR)/**/tableSchemaManagement/*.java $(SOURCEDIR)/**/test/*.java)
classes = $(sources:.java=.class)

preparation: clean
	mkdir -p ${OUTDIR}

clean:
	rm -rf ${OUTDIR}

%.class: %.java
	$(JAVAC) -d "$(OUTDIR)" -cp "$(OUTDIR):$(CLASSPATH)" $<

part1Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore CSCI485ClassProject.test.Part1Test

.PHONY: part1Test clean preparation