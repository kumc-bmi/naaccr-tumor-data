FROM python:3.7

RUN pip install --no-cache-dir py-make

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ARG ARCHIVE

ADD build/${ARCHIVE} .

# ISSUE: refactor overlap with Makefile check_code
# pymake lacks support for basic x: y dependencies. :-/
# https://github.com/tqdm/py-make/issues/2
CMD ["pymake", "doctest", "lint", "static", "freeze"]
