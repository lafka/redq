.PHONY: deps test

REBAR ?= rebar
DIALYZER ?= dialyzer
ERL ?= erl
CONFIG ?= pooler.config

all: deps compile

deps:
	$(REBAR) get-deps

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	public_key eunit syntax_tools compiler

COMBO_PLT = .dialyzer.plt

check_plt: compile
	$(DIALYZER) --check_plt --plt $(COMBO_PLT) --apps $(APPS)

build_plt: compile
	$(DIALYZER) --build_plt --output_plt $(COMBO_PLT) --apps $(APPS)

dialyzer: compile
	@$(DIALYZER) --plt $(COMBO_PLT) -Wno_return --src src

test: test-ct test-eunit

test-ct:
	$(REBAR) skip_deps=true ct

test-eunit:
	$(REBAR) skip_deps=true eunit suite=$(suite)

distclean: clean
	$(REBAR) delete-deps

run:
	$(ERL) \
		-pa ebin deps/*/ebin \
		-eval '[application:ensure_started(App) || App <- [eredis, pooler, redq]].'
