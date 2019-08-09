# Design, development notes for contributors

Exploratory design is done using pyspark and jupyter notebook.

*Bootstrap notes below are still a bit rough.*

---

## PySpark and jupyter notebooks vs. sqldeveloper

  - jupyter notebooks are great for exploration, visualization
  - Spark / Jupyter integration is a bit klunky: restarts...
  - sqldeveloper features I miss: selecting and running a subquery

---

## JVM Dependency: Apache Spark

[Download spark][dl], unpack it, and set `SPARK_HOME` to match. Our
testing has been with spark 2.2.1.

[dl]: https://spark.apache.org/downloads.html

As noted in [a tutorial][tut1]:

```
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
```

[tut1]: https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f


---

### Java 8 still OK

We have no dependencies on more recent versions, yet.

---

### Road not taken: OHDSI SQLRender

[OHSDSI SQLRender](https://ohdsi.github.io/SqlRender/) is a nifty
approach:

  - Write in MS Sql Server dialect
  - tool translates to Oracle, postgres, sqlite, ...
  - somewhat ad-hoc: based on a CSV file of rules

challenge: sqlserver IDE? Intellij?

---

## luigi replaces paver automation library

In production, we use luigi `PySparkTask`.

  - GROUSE experience: 70B+ facts in ~2 weeks
    - task visualizer is nice
    - resilient to failure
    - preserves partial progress
  - paver execution is serialized
    - paver has gone fallow
  - luigi tasks need a `complete` method as well as a `run` method
    - one approach: save `task_id` in i2b2 `upload_status` record

---

## QA with doctest and mypy

  - ETL code is often hard to unit test: mocking a DB is expensive
  - static type checking finds typos etc.
  - types provide documentation

```
export PATH=~/.conda/envs/pytr3/bin:$PATH; && \
  python -m doctest tumor_reg_summary.py && \
  mypy --strict tumor_reg_summary.py && \
  python tumor_reg_summary.py naaccr_ddict/record_layout.csv tr1.db NCDB_Export.txt
```

---

### Python coding style

  - We follow python community norms: PEP8; use flake8 to check.
    - **ISSUE**: explain how to check; establish Jenkins ci.
  - The first line of a module or function docstring should be a short description
    - if it is blank, either the item is in an early stage of
      development or the name and doctests are supposed to make the purpose
      obvious.

  - *NOTE*: with static type annotations, the 79 character line
            length limit is awkward; in GROUSE we used 99 in `setup.cfg`.

---

## OCap discipline

For **robust composition** and **cooperation without vulnerability**,
we use [object capability (ocap) discipline][ocap]:

  - **Memory safety and encapsulation**
    - There is no way to get a reference to an object except by
      creating one or being given one at creation or via a message; no
      casting integers to pointers, for example.
    - From outside an object, there is no way to access the internal
      state of the object without the object's consent (where consent
      is expressed by responding to messages).

  - **Primitive effects only via references**
    - The only way an object can affect the world outside itself is
      via references to other objects. All primitives for interacting
      with the external world are embodied by primitive objects and
      **anything globally accessible is immutable data**. There must
      be no `open(filename)` function in the global namespace, nor may
      such a function be imported.

[ocap]: http://erights.org/elib/capability/ode/ode-capabilities.html

OCap discipline is consistent with the "don't call us, we'll call you"
style that facilitates unit testing with mocks.

---

### OCap discipline in python

  - **Memory safety and encapsulation**
      - Python is memory-safe.
      - Python doesn't enforce encapsulation; it's a coding convention.
        - vs. python community norm: "we're all adults here"
  - **Primitive effects only via references**
      - Python doesn't enforce this; it's a coding convention
      - Only access powerful functions inside `if __name__ == '__main__':`
         - I/O:
           - pass around `pathlib.Path` objects instead of using `open(str)`,
             which is like casting an int to a pointer
           - pass around `urllib.urlopener` objects instead of using `urlopen(str)`
             - bonus: wrap urllib objects in pathlib API
        - other sources of non-determinism: random numbers, global mutable state

---

### Luigi serializable tasks vs. OCap discipline

  - *ISSUE*: Luigi's design seem to clash with OCap discipline.
             Constructors are implict and tasks parameters have to be
             serializable, which works against the "don't call us,
             we'll call you" idiom.  Also, the task cache is global
             mutable state.

---

### Required python version: 3.7

  - Spark, pandas are among [projects that
    pledge to drop Python 2 support in or before
    2020](https://python3statement.org/)
  - `importlib.resources` is new in 3.7
    - back-port is available if we want to support 3.6

---

## Context: Jenkins

  - Our shop uses Jenkins
  - beyond the scope of this document
  - idea: use jenkinsfiles and check them in here?


---

### Wish: docker for integration testing

  - spin up a complete i2b2 DB anywhere
  - middle tier, web client are mostly done already


---

### Bootstrap with anaconda and direnv

[Install miniconda][mc] and create a `py3tr` environment:

[mc]: https://docs.conda.io/en/latest/miniconda.html

```
$ bash ./Miniconda3-latest-Linux-x86_64.sh -b -p ~/opt/miniconda3
$ ~/opt/miniconda3/bin/conda init  # set up $PATH; start a new shell...
$ conda create -n py3tr python=3.7
```

(tested with: `Miniconda3-latest-Linux-x86_64.sh`)

**ISSUE**: migrating from `requirements.txt` to `environment.yml`;
see bootstrapping below

**ISSUE**: migrate to **pipenv** and `Pipfile`?

Extra dev requirements: `pip install flake8 mypy`.

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
