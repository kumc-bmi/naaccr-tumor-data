check_code: doctest lint static

release: check_code build run_notebook archive

build:
	mkdir -p build


# TODO: ontology release artifact

test_data_etl: build/naaccr_observations.csv build/naaccr_export_stats.csv

build/naaccr_observations.csv: build/tumor_reg_data_run.ipynb

build/naaccr_export_stats.csv: build/tumor_reg_data_run.ipynb

##
# Unit testing with doctest
#
# IDEA: nosetests --with-doctest
# IDEA: or tox?
# IDEA: __pycache__/tabular.cpython-37.pyc depends tabular.py; doctest, mypy it
DOCTEST_FILES=tabular.py sql_script.py tumor_reg_ont.py tumor_reg_data.py tumor_reg_tasks.py
doctest:
	for f in $(DOCTEST_FILES); do echo $$f; python -m doctest $$f; done

testandtype:
	for f in $(DOCTEST_FILES); do echo $$f; python -m doctest $$f; mypy --strict $$f; done

##
# Python community style
lint:
	flake8 .

lint-deps:
	pip install

##
# Static typechecking
static:
	mypy --strict .

static-type-deps:
	pip install

##
# TODO, ISSUE markers
SOURCES=$(shell git ls-tree -r --name-only --full-tree HEAD)
TODO.md: $(SOURCES)
	@egrep -n 'TODO:|ISSUE:|@@' $(SOURCES) >$@

##
# Derived requirements.txt
freeze:
	sh -c 'pip freeze >/var/spool/requirements.txt'

##
# Source Archive
#
# Turns out github does this automatically. Oh well.
VERSION := $(shell git log --pretty=format:'%h' HEAD -n1)
ARCHIVE=naaccr-tumor-reg-$(VERSION).tgz

archive: build/$(ARCHIVE)

build/$(ARCHIVE): build
	git archive -o $@ HEAD

##
# Check source release in a clean docker environment
# ISSUE: user ids, permissions
docker-check: build/$(ARCHIVE)
	docker build -t tr-check --build-arg ARCHIVE=$(ARCHIVE) .
	docker run --rm --name check-$(VERSION) -v /tmp:/var/spool tr-check make check_code freeze

docker-notebook: build/$(ARCHIVE)
	docker build -t tr-check --build-arg ARCHIVE=$(ARCHIVE) .
	docker run --rm --name notebook-$(VERSION) \
		-p 4090:4040 -p 4091:4041 -p 8899:8888 \
		tr-check

##
# Integration testing with jupyter notebook
run_notebook: build/tumor_reg_data_run.html

build/tumor_reg_data_run.html: build/tumor_reg_data_run.ipynb
	jupyter nbconvert --to html $< $@

build/tumor_reg_data_run.ipynb: tumor_reg_data.ipynb
	PYSPARK_DRIVER_PYTHON=python $(SPARK_HOME)/bin/spark-submit run_nb.py $< build/ $@

tumor_reg_data.ipynb: tumor_reg_data.py
	jupytext --to notebook $<

##
# gpc.Loader testing
test-loader: src/test/resources/data.jsonl ./build/libs/naaccr-tumor-data.jar
	ID_URL=jdbc:oracle:thin:@localhost:8621:$(ORACLE_SID) \
	ID_USER=$(LOGNAME) \
	ID_DRIVER=oracle.jdbc.OracleDriver \
	java -jar ./build/libs/naaccr-tumor-data.jar \
		gpc.Loader --account ID --load <src/test/resources/data.jsonl

./build/libs/naaccr-tumor-data.jar: src/main/groovy/gpc.Loader.groovy
	./gradlew test
	./gradlew fatjar

