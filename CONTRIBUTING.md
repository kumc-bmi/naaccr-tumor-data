# Design, development notes for contributors

After exploring Apache Spark and PySpark, we've decided to stay close to the JVM / JDBC platform used by i2b2.


## build.gradle and IntelliJ IDEA

The groovy code is typically developed with IDEA, but `build.gradle` works anywhere that Java does,
especially with `./gradlew` or `./gradlew.bat`. IDEA's "optimize imports" and "reformat code" are
handy; here's hoping we can teach gradle to check style.

The `fatjar` task bundles JDBC drivers used in i2b2: postgres, MS SQL Server, and Oracle.


## H2 Database Engine for portable ETL

Much of the Oracle-specific ETL code from HERON has been ported to H2. We expect an in-memory DB to suffice
for production use, though we often use a local on-disk DB for integration testing.

We used [HSQLDB](https://mvnrepository.com/artifact/org.hsqldb/hsqldb) until we ran into trouble
and switched to H2. We evaluated [Apache Derby](https://mvnrepository.com/artifact/org.apache.derby/derby) but
it was pretty much a non-starter.


## Open Source SQL IDE: dbeaver

We find [https://dbeaver.io/ DBeaver] a handy SQL development environment.


## Groovy is a smooth transition from python, but Java is catching up

  - pro
    - nice Sql, JSON APIs
    - easy porting from python
      - can prototype in unityped mode
  - con
    - IDE isn't as quick to spot type errors
    - spread operator doesn't work with @CompileStatic
    - @CompileStatic requires `Table t = new Table()` stutter,
      which is mitigated in recent Java versions.
    - recent Java versions also have lambdas, which mitigates
      the advantage of groovy's `{ x -> x + 1}` closure syntax
      (which is only typesafe in the return value, not the arguments).


## docopt: Usage documentation in README.d is code

Note the [docopt](https://mvnrepository.com/artifact/com.offbytwo/docopt) dependency in `build.gradle`
and `TumorFileTest.testDocOpt`. The `buildUsageDoc` gradle task extracts the usage doc from `README.md`
and puts it in `src/main/resources/usage.txt`.


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

### OCap discipline in groovy

  - **Memory safety and encapsulation**
      - Groovy, like all JVM languages, is memory-safe (aside from JNI)
      - Groovy can enforce encapsulation with `private`
        (`protected` is awkward; though python inheritance habits die
          hard, composition is usually cleaner than inheritance anyway.)
  - **Primitive effects only via references**
      - Groovy doesn't enforce this; it's a coding convention.
        The Java standard library is rife with ambient authority.
        In fact, groovy makes it worse: every string has
        [`.execute`](https://docs.groovy-lang.org/latest/html/groovy-jdk/java/lang/String.html#execute())
        i.e. it carries authority to launch subprocesses!
      - Only access powerful functions inside `main()`
         - I/O:
           - pass around `URL` or `Path` objects instead of using `open(str)`,
             which is like casting an int to a pointer
        - other sources of non-determinism: random numbers, global mutable state

---

## Transitioning from python

Each of the main modules has unit tests: `python -m doctest
$MODULE.py` for:

 - `tumor_reg_ont.py`
 - `tumor_reg_data.py`
 - `tumor_reg_tasks.py`
 - `sql_script.py`

All contributions should also pass the `lint` and `static` checks
discussed below. Use `make` or `make code_check` to check all three.

*Bootstrap notes below are still a bit rough.*

---

## PySpark and jupyter notebooks vs. sqldeveloper

Exploratory design was done using pyspark and jupyter notebook.

  - jupyter notebooks are great for exploration, visualization
  - A `local` spark session makes a `metastore_db` in the current
    directory, which effectively limits pyspark notebooks to one per
    directory.
  - sqldeveloper features I miss: selecting and running a subquery
    - see SQL IDE below
  - troubleshooting pain:
    - pyspark: Java/Scala stack-traces inside python stack traces
    - having SQL be a 2nd-class citizen in notebooks
    - having the Oracle world and the spark world split

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

## Troubleshooting: Dev UX :-/

  - python/java bridge (py4j) spits out huge java stack traces
  - luigi re-tries 5x on error
  - SQL syntax error easily gets lost

---

## Static type checking with mypy (`static`)

  - use `mypy --strict .` or `make static`.
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

### Python coding style (`lint`)

  - We follow python community norms: PEP8; use `flake8 .` or `make lint` to check.
  - The first line of a module or function docstring should be a short description
    - if it is blank, either the item is in an early stage of
      development or the name and doctests are supposed to make the purpose
      obvious.

  - *NOTE*: with static type annotations, the 79 character line
            length limit is awkward; see `max-line-length` in `setup.cfg`.

  - *NOTE*: Though imports at the top of a notebook cell are handy for
            prototyping, we move them to the top of the notebook (module)
            to conform to community style.

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

### Wish: integration testing with an i2b2 front-end

using docker?

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

