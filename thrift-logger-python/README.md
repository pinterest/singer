== Get started ==

```lang=bash
# create virtualenv
$ cd thrift_logger
$ virtualenv .venv

# init virtualenv
$ . .venv/bin/activate

# install dependencies
$ pip install -r requirements.txt

# run unittest
$ nosetests
# or
$ make test
# or, if necessary
$ .venv/bin/nosetests

# run linter
$ make lint
```

Note you might need to config `~/.pip/pip.config` to add

```
[global]
index-url = $pypi_epository_url
timeout=3
```

== How to update schemas ==

thrift logger depends on schemas. Sure make sure the schemas files are up-to-date by:

```
make schemas
```

== Local install ==

In case you want to test thrift logger change with other repo, do

```
# make sure you are in the right virtualenv
$ . <target virtualenv>/bin/activate

# make sure dependencies are installed
$ pip install -r requirements.txt

# install thrift-logger
$ python setup.py install
```
