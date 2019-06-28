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


### Python dependencies

See `requirements.txt`.


### Bootstrap with direnv

Build a `.envrc` based on `.envrc.template`; then:

```
~/projects/naaccr-tumor-data$ direnv allow
direnv: loading .envrc
Running virtualenv with interpreter /usr/bin/python3
Using base prefix '/usr'
...
Installing setuptools, pip, wheel...
done.
direnv: export +VIRTUAL_ENV ~PATH

~/projects/naaccr-tumor-data$ pip install -r requirements.txt 
...
Successfully installed py4j-0.10.7 pyspark-2.4.3
```

Then run `pyspark` to launch a jupyter notebook web server.


Notebooks start with `spark` bound to a spark context. By default, a
derby `metastore_db` is created in the current working directory.
