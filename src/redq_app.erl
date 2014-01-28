-module(redq_app).

-behaviour(application).

-export([
	  start/2
	, stop/1]).


start(_StartType, _StartArgs) ->
	random:seed(os:timestamp()),
	redq_sup:start_link().


stop(_State) ->
	ok.
