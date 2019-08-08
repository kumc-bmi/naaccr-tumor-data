luigi tasks
  - staging
    - staging validation
      - stats; e.g. ~50% male/female, x% breast cancer, x cases/year, ...
        - charts?


### ISSUE: Platform

  - HERON ETL platform is Jenkins + python2 + paver + Oracle SQL with
    some pandas and some spark; migrating
    - from paver to luigi
    - from python2 to python3 (with mypy)
      - Spark, pandas are among [projects that
        pledge to drop Python 2 support in or before
        2020](https://python3statement.org/)
    - from Oracle SQL to... postgres? to data-lake?
      - from sqlplus to sqlcl
      - sql loader isn't essential at O(100K) records
    - from Java 8 to Java 11
  - Docker? for integration testing?
    - stand-alone i2b2 web client, middle tier?
    - stand-alone DB?

### ISSUE: DB Interop

  - Apache Spark runs on the JVM, the i2b2 platform

  - [OHSDSI SQLRender](https://ohdsi.github.io/SqlRender/) is a nifty
    approach: based on a CSV file of rules.


## Exploratory Notebook

Currently we're using pyspark and jupyter notebook to explore the
design space.


## QA with doctest and mypy

```
export PATH=~/.conda/envs/pytr3/bin:$PATH; && \
  python -m doctest tumor_reg_summary.py && \
  mypy --strict tumor_reg_summary.py && \
  python tumor_reg_summary.py naaccr_ddict/record_layout.csv tr1.db NCDB_Export.txt
```

## pyspark

As noted in [a tutorial][tut1]:

```
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
```

[tut1]: https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f


## Dependencies: spark, pyspark, pandas

[Download spark][dl], unpack it, and set `SPARK_HOME` to match. Our
testing has been with spark 2.2.1.

[dl]: https://spark.apache.org/downloads.html


### Python dependencies (WIP)

ISSUE: migrating from `requirements.txt` to `environment.yml`.

Extra dev requirement: `pip install flake8`.
**ISSUE**: migrate to **pipenv** and `Pipfile`?


### Bootstrap with anaconda and direnv

[Install miniconda][mc] and create a `py3tr` environment:

[mc]: https://docs.conda.io/en/latest/miniconda.html

```
$ bash ./Miniconda3-latest-Linux-x86_64.sh -b -p ~/opt/miniconda3
$ ~/opt/miniconda3/bin/conda init  # set up $PATH; start a new shell...
$ conda create -n py3tr python=3.7
```

(tested with: `Miniconda3-latest-Linux-x86_64.sh`)

Make an `.envrc` based on `.envrc.template`; install
[layout_anaconda][la] in `~/.direnvrc`; then:

[la]: https://github.com/direnv/direnv/wiki/Python#anaconda

```
~/projects/naaccr-tumor-data$ direnv allow
direnv: loading .envrc
direnv: export +CONDA_PREFIX_1 +JAVA_HOME ...

~/projects/naaccr-tumor-data$ pip install -r requirements.txt 
...
Successfully installed MarkupSafe-1.1.1 Send2Trash-1.5.0 attrs-19.1.0 backcall-0.1.0 bleach-3.1.0 decorator-4.4.0 defusedxml-0.6.0 docutils-0.15.2 entrypoints-0.3 ipykernel-5.1.2 ipython-7.7.0 ipython-genutils-0.2.0 ipywidgets-7.5.1 jedi-0.14.1 jinja2-2.10.1 jsonschema-3.0.2 jupyter-1.0.0 jupyter-client-5.3.1 jupyter-console-6.0.0 jupyter-core-4.5.0 lockfile-0.12.2 luigi-2.8.7 mistune-0.8.4 nbconvert-5.5.0 nbformat-4.4.0 notebook-6.0.0 numpy-1.17.0 pandas-0.24.2 pandocfilters-1.4.2 parso-0.5.1 pexpect-4.7.0 pickleshare-0.7.5 prometheus-client-0.7.1 prompt-toolkit-2.0.9 ptyprocess-0.6.0 py4j-0.10.7 pygments-2.4.2 pyrsistent-0.15.4 pyspark-2.4.3 python-daemon-2.1.2 python-dateutil-2.8.0 pytz-2019.2 pyzmq-18.1.0 qtconsole-4.5.2 six-1.12.0 terminado-0.8.2 testpath-0.4.2 tornado-4.5.3 traitlets-4.3.2 wcwidth-0.1.7 webencodings-0.5.1 widgetsnbextension-3.5.1
```

Then run `pyspark` to launch a jupyter notebook web server.


Notebooks start with `spark` bound to a spark context. By default, a
derby `metastore_db` is created in the current working directory.
