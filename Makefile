.PHONY: all clean test

all: compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

test: compile
	./rebar eunit skip_deps=true
