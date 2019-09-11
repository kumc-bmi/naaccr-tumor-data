
check_code: doctest lint static

release: check_code run_notebook

# TODO: code archive release artifact
# TODO: ontology release artifact
# TODO: transformed data release artifact

run_notebook: tumor_reg_data_run.html

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

tumor_reg_data_run.html: tumor_reg_data_run.ipynb
	jupyter nbconvert --to html $< $@

tumor_reg_data_run.ipynb: tumor_reg_data.ipynb
	PYSPARK_DRIVER_PYTHON=python spark-submit run_nb.py $< $@

tumor_reg_data.ipynb: tumor_reg_data.py
	jupytext --to notebook $<
