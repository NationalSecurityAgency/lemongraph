PYTHON=pypy
PYTHON_CFLAGS=-O3 -Wall
CC+=-pipe
CFLAGS=-fPIC -Wall -Wunused-variable -Wunused-but-set-variable -O3
LMDB:=deps/lmdb/libraries/liblmdb
CPPFLAGS+=-I$(LMDB) -Ilib
VPATH=lib:$(LMDB)
SNAPSHOT:=lg-$(shell date +%Y%m%d)

default: build

liblemongraph.a:  mdb.o midl.o lemongraph.o db.o counter.o afsync.o avl.o
liblemongraph.so: mdb.o midl.o lemongraph.o db.o counter.o afsync.o avl.o
liblemongraph.so: LDFLAGS=-pthread
liblemongraph.so: LDLIBS=-lz
$(LMDB)/mdb.c:    deps
$(LMDB)/midl.c:   deps
db.o:             deps

clean:
	@find . -type d \( -name __pycache__ -o -name .eggs -o -name build -o -name dist -o -name \*.egg-info \) -exec rm -rf {} \; -print -prune
	@find . -type f \( -name \*.pyc -o -name MANIFEST -o -name \*.o -o -name \*.a -o -name \*.so \) -delete -print

distclean: clean
	@find ./deps -mindepth 1 -maxdepth 1 -type d -exec find {} -mindepth 1 -delete \; -delete -print

deps:
	@$(MAKE) -C deps --no-print-directory

deps-update:
	@$(MAKE) -C deps --no-print-directory update

test: test.py deps
	CFLAGS="$(PYTHON_CFLAGS)" $(PYTHON) $<

install:
	CFLAGS="$(PYTHON_CFLAGS)" $(PYTHON) setup.py install

uninstall:
	CFLAGS="$(PYTHON_CFLAGS)" $(PYTHON) setup.py uninstall

sdist: deps
	CFLAGS="$(PYTHON_CFLAGS)" $(PYTHON) setup.py sdist

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

.PHONY: install uninstall test sdist deps deps-update

.PHONY: build build-examples build-bench

examples/example: liblemongraph.a examples/example.c
	@$(CC) $(CFLAGS) $(CPPFLAGS) -lz -pthread -L. -llemongraph \
		examples/example.c -o examples/example

bench/bench: liblemongraph.a bench/bench.c
	@$(CC) $(CFLAGS) $(CPPFLAGS) -lz -pthread -L. -llemongraph \
		bench/bench.c -o bench/bench

build-examples: examples/example
build-bench: bench/bench
build: build-examples build-bench
	CFLAGS="$(PYTHON_CFLAGS)" $(PYTHON) setup.py build

.PHONY: run-example.c run-example.py run-bench.c run-bench.py

run-example.c:
	examples/example
run-example.py:
	$(PYTHON) examples/example.py
run-bench.c:
	bench/bench
run-bench.py:
	$(PYTHON) examples/example.py