.PHONY: schemas

clean: clean_pyc
	rm -f topic.*
	rm -rf build
	rm -rf dist

clean_pyc:
	find . -name "*.pyc" -delete

install_pip_runtime_dependencies:
	pip install -r ./requirements.txt

uninstall_pip_runtime_dependencies:
	pip freeze | grep -v "^-f" | xargs pip uninstall -y

install_lint_dependencies:
	pip install -r ./requirements_lint.txt

lint: install_lint_dependencies
	./tools/pyflakes.sh
	./tools/pep8.sh

test_unittests: install_pip_runtime_dependencies
	nosetests

test: test_unittests

update_schemas:
	rm -rf $(SCHEMASPATH)
	git clone ssh://git@phabricator.pinadmin.com/diffusion/S/thrift-schemas.git $(SCHEMASPATH)

SCHEMASPATH = schemas_src

THRIFT_0_8_BIN ?= thrift

THRIFT_FILES = singer_if

COMPILE_THRIFT = $(THRIFT_0_8_BIN) -r --verbose -I schemas_src/commons/thrift -I schemas_src/schemas/thrift -I schemas_src -out thrift_logger --gen py:dynamic,dynbase=TBase,dynimport='from thrift.protocol.TBase import TBase; from thrift.protocol.TBase import TExceptionBase; from thrift.Thrift import TApplicationException',slots schemas_src/schemas/thrift/$(thrift_file).thrift

compile_schemas:
	sh ./test_thrift_version.sh $(THRIFT_0_8_BIN) "Thrift version 0.8.0"
	$(foreach thrift_file, $(THRIFT_FILES), \
	 	$(COMPILE_THRIFT);) \
	rm -f __init__.py

clean_schemas:
	rm -rf $(SCHEMASPATH)

schemas: update_schemas compile_schemas clean_schemas
