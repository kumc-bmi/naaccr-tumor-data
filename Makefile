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
#
# Note: for compatibility with pymake, we use python rather than shell
# syntax for this loop.
DOCTEST_FILES=tumor_reg_ont.py tumor_reg_data.py tumor_reg_tasks.py sql_script.py
doctest:
	python -c 'from subprocess import check_call;\
                   [print(f) or check_call(["python", "-m", "doctest", f])\
                    for f in "$(DOCTEST_FILES)".split()]'

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
	docker run --rm --name check-$(VERSION) tr-check

##
# Integration testing with jupyter notebook
run_notebook: build/tumor_reg_data_run.html

build/tumor_reg_data_run.html: build/tumor_reg_data_run.ipynb
	jupyter nbconvert --to html $< $@

build/tumor_reg_data_run.ipynb: tumor_reg_data.ipynb
	PYSPARK_DRIVER_PYTHON=python spark-submit run_nb.py $< build/ $@

tumor_reg_data.ipynb: tumor_reg_data.py
	jupytext --to notebook $<
