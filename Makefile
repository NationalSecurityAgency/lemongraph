PYTHON=python
CC+=-pipe
CFLAGS=-fPIC -Wall -Wunused-variable -Wunused-but-set-variable -O3
CPPFLAGS+=-Ideps/lmdb/libraries/liblmdb -Ilib
VPATH=lib:deps/lmdb/libraries/liblmdb
SNAPSHOT:=lg-$(shell date +%Y%m%d)

default: build

liblemongraph.a:  mdb.o midl.o lemongraph.o db.o
liblemongraph.so: mdb.o midl.o lemongraph.o db.o
liblemongraph.so: LDFLAGS=-pthread
liblemongraph.so: LDLIBS=-lz

clean:
	@echo $(wildcard *.a *.so *.o *.pyc LemonGraph/*.pyc LemonGraph/*.so MANIFEST) | xargs --no-run-if-empty rm -v
	@echo $(wildcard .eggs build dist LemonGraph/__pycache__ LemonGraph.egg-info)  | xargs --no-run-if-empty rm -rv

distclean: clean
	@find deps -mindepth 2 -maxdepth 2 -exec rm -rv {} \;

deps:
	@$(PYTHON) setup.py check

deps-update:
	@git submodule init
	@git submodule update --remote

build:
	$(PYTHON) setup.py build

test: test.py deps
	$(PYTHON) $<

install:
	$(PYTHON) setup.py install

uninstall:
	$(PYTHON) setup.py uninstall

sdist:
	$(PYTHON) setup.py sdist

snapshot:
	@rm -rf $(SNAPSHOT) $(SNAPSHOT).zip
	@git clone . $(SNAPSHOT)
	@$(MAKE) -C $(SNAPSHOT) deps
	@zip -q -r9 $(SNAPSHOT).zip $(SNAPSHOT)
	@rm -rf $(SNAPSHOT)
	@echo $(SNAPSHOT).zip

lib%.so: %.o
	$(LINK.o) -shared $^ $(LOADLIBES) $(LDLIBS) -o $@

lib%.a: %.o
	$(AR) rcs $@ $^

.PHONY: build install uninstall test sdist deps deps-update
