
check_code: doctest lint static

VERSION := $(shell git log --pretty=format:'%h' HEAD -n1)
ARCHIVE=build/naaccr-tumor-reg-$(VERSION).zip

release: check_code build run_notebook $(ARCHIVE)

build:
	mkdir -p build

archive: $(ARCHIVE)

$(ARCHIVE): build
	git archive -o $(ARCHIVE) HEAD

# TODO: ontology release artifact

test_data_etl: build/naaccr_observations.csv build/naaccr_export_stats.csv

build/naaccr_observations.csv: build/tumor_reg_data_run.ipynb

build/naaccr_export_stats.csv: build/tumor_reg_data_run.ipynb

run_notebook: build/tumor_reg_data_run.html

DOCTEST_FILES=tumor_reg_ont.py tumor_reg_data.py tumor_reg_tasks.py sql_script.py

doctest:
	# nosetests --with-doctest
	# or tox?
	for f in $(DOCTEST_FILES); do echo $$f; python -m doctest $$f; done

lint:
	flake8 .

static:
	mypy --strict .
	# previous approaches
	# mypy --strict --show-traceback .
	# MYPYPATH=.:$(CONDA_PREFIX)/lib/python3.7 mypy --strict .

build/tumor_reg_data_run.html: build/tumor_reg_data_run.ipynb
	jupyter nbconvert --to html $< $@

build/tumor_reg_data_run.ipynb: tumor_reg_data.ipynb
	PYSPARK_DRIVER_PYTHON=python spark-submit run_nb.py $< build/ $@

tumor_reg_data.ipynb: tumor_reg_data.py
	jupytext --to notebook $<
