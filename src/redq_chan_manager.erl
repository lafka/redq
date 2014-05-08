-module(redq_chan_manager).

%% API

-compile({no_auto_import, [whereis/1]}).

-export([
	  add/3
	, add/4
	, destroy/1
	, whereis/1
	]).

-export([
	  start_link/0
	, init/1 % Gen server
	, handle_call/3
	, handle_cast/2
	, handle_info/2
	, terminate/2
	, code_change/3
]).

%% Useful
-export([
	  stop/0
	]).

-define(sup, redq_chan_sup).

add(Chan, Patterns, Opts) ->
	add(Chan, Patterns, self(), Opts).

add(Chan, Patterns, Parent, Opts) ->
	gen_server:call(?MODULE, {add, [Chan, Patterns, Parent, Opts]}).

destroy(Chan) ->
	destroy(Chan, 1500).

destroy(Chan, Timeout) ->
	gen_server:call(?MODULE, {destroy, Chan, Timeout}).

whereis(Chan) ->
	gen_server:call(?MODULE, {whereis, Chan}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() -> gen_server:call(?MODULE, stop).

init([]) ->
	{ok, dict:new()}.

handle_call({add, [Chan, Patterns, Parent, Opts]}, _From, State) ->
	case redq_chan:new(Chan, Patterns, Parent, Opts) of
		{ok, Pid} ->
			{reply, {ok, Chan}, dict:append(Chan, Pid, State)};

		{error, _} = Err ->
			{reply, Err, State}
	end;
handle_call({destroy, Chan, Timeout}, _From, State) ->
	case dict:find(Chan, State) of
		{ok, [Pid]} ->
			Ref = monitor(process, Pid),
			_ = supervisor:terminate_child(?sup, Pid),

			receive {'DOWN', Ref, process, Pid, _Reason} ->
				NewState = dict:erase(Chan, State),
				{reply, ok, NewState}
			after Timeout ->
				{reply, {error, timeout}, State}
			end;

		error ->
			{reply, {error, notfound}, State}
	end;
handle_call({whereis, Chan}, _From, State) ->
	case dict:find(Chan, State) of
		{ok, [Pid]} ->
			{reply, {ok, Pid}, State};

		error ->
			{reply, {error, notfound}, State}
	end;
handle_call(stop, _From, State) ->
	{stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
	{noreply, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Msg, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
