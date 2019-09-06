
all: check_code run_notebook

run_notebook: tumor_reg_data_run.html

check_code:
	# nosetests --with-doctest
	# or tox?
	python -m doctest tumor_reg_ont.py
	python -m doctest tumor_reg_data.py
	python -m doctest tumor_reg_tasks.py
	python -m doctest sql_script.py

check_code_todo: static lint

lint:
	pyflakes .

static:
	MYPYPATH=. mypy --strict --show-traceback heron_staging/tumor_reg/seer_recode.py
	MYPYPATH=. mypy --strict --show-traceback tumor_reg_ont.py
	# mypy --strict --show-traceback .
	# echo MYPYPATH=$(CONDA_PREFIX)/lib/python3.7 mypy --strict .
	# MYPYPATH=.:$(CONDA_PREFIX)/lib/python3.7 mypy --strict .

tumor_reg_data_run.html: tumor_reg_data_run.ipynb
	jupyter nbconvert --to html $< $@

tumor_reg_data_run.ipynb: tumor_reg_data.ipynb
	PYSPARK_DRIVER_PYTHON=python spark-submit run_nb.py $< $@

tumor_reg_data.ipynb: tumor_reg_data.py
	jupytext --to notebook $<
