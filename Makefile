PACKAGE ?= pipesock
VERSION ?= $(shell git describe --tags)
BASE_DIR ?= $(shell pwd)
ERLANG_BIN ?= $(shell dirname $(shell which erl))
REBAR ?= $(shell pwd)/rebar3
MAKE = make

.PHONY: compile rel clean packageclean check lint shell

all: compile

compile:
	$(REBAR) compile

rel: compile
	$(REBAR) release

relclean:
	rm -rf _build/default/rel

clean: packageclean
	$(REBAR) clean

packageclean:
	rm -fr *.deb
	rm -fr *.tar.gz

check: xref dialyzer lint

lint:
	$(REBAR) as lint lint

shell:
	$(REBAR) shell --apps pipesock

include tools.mk

