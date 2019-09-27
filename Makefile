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
DOCTEST_FILES=tumor_reg_ont.py tumor_reg_data.py tumor_reg_tasks.py sql_script.py
doctest:
	for f in $(DOCTEST_FILES); do echo $$f; python -m doctest $$f; done

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
