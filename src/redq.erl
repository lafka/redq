-module(redq).

-export([
	  push/2, push/3
	, consume/1, consume/2
	, destroy/1
	, peek/1, peek/2
	, take/1, take/2
	, flush/1
	, remove/2
	, size/1
	, meta/2, meta/3, meta/4
]).

-type queue() :: [binary()].
-type channel() :: binary().
-type sel() :: {queue, queue()} | {chan, channel()}.
-type event() :: binary().
-type events() :: [event()].

-type push_opts() :: [{ttl, non_neg_integer()}].
-type consume_opts() :: [term()].
-type take_opts() :: peek_opts().
-type peek_opts() :: [{slice, Pos :: non_neg_integer(), Len :: non_neg_integer()}
	| {slice, Len :: non_neg_integer()}
	| {n, Pos :: non_neg_integer()}].

-export_type([channel/0, event/0]).


-spec push(sel(), event()) ->
	ok | {error, Err} when Err :: term().

push(Sel, Event) ->
	push(Sel, Event, []).


-spec push(sel(), event(), push_opts()) ->
	ok | {error, Err} when Err :: no_connection.

% Add event to queue and push notification to all channels
push({queue, Queue0}, Event, _Opts) ->
	{QMap, Queue} = {key(Queue0, queuemap), key(Queue0, queue)},
	{Pid, Cont} = get_pid(),


	case eredis:qp(Pid, [["SMEMBERS", QMap], ["SADD", Queue, Event]]) of
		[{ok, _Chans}, {ok, <<"0">>}] -> % Event exists in queue
			ok;

		[{ok, []}, _] -> % No channels
			ok;

		[{ok, Chans}, {ok, <<"1">>}] ->
			[{ok,_}|_] = eredis:qp(Pid, [
				["PUBLISH", Chan, Event] || Chan <- Chans
			])
	end,

	_ = Cont(Pid),
	ok;

push({chan, Chan}, Event, Opts) ->
	with_chan(Chan, fun(Pid, Queues) ->
		Ops = case lists:member(volatile, Opts) of
			true ->
				[];

			false ->
				[["SADD", key(Q, queue), Event] || Q <- Queues]
		end,

		case eredis:qp(Pid, [["PUBLISH", Chan, Event] | Ops]) of
			[{ok, _} | _] ->
				ok;

			{error, _} = Err ->
				Err
		end
	end).

-spec take(sel()) ->
	{ok, events()} | {error, Err} when Err :: term().

take(Sel) ->
	take(Sel, []).


-spec take(sel(), take_opts()) ->
	{ok, events()} | {error, Err} when Err :: term().

take({chan, Chan}, Opts) ->
	%% @todo 2013-09-09; lafka - validate that items are removed
	with_chan(Chan, fun(Pid, Queues) ->
		{ok, Items0} = eredis:q(Pid, ["SUNION" | [ key(Q, queue)|| Q <- Queues]]),

		{ok, Items} = return_items(Items0, Opts),

		Ops = lists:foldl(fun(Queue, Acc) ->
			[["SREM", key(Queue, queue), I] || I <- Items] ++ Acc
		end, [], Queues),

		case eredis:qp(Pid, Ops) of
			[{ok, _}|_] ->
				{ok, Items};

			[] ->
				{ok, []}
		end
	end);

take({queue, Queue0}, Opts) ->
	Queue = key(Queue0, queue),

	{Pid, Cont} = get_pid(),
	{ok, Items0} = eredis:q(Pid, ["SMEMBERS", Queue]),

	{ok, Items} = return_items(Items0, Opts),

	case eredis:qp(Pid, [["SREM", Queue, I] || I <- Items]) of
		[{ok, _}|_] ->
			_ = Cont(Pid),
			{ok, Items};

		[] ->
			_ = Cont(Pid),
			{ok, []}
	end.


-spec peek(sel()) ->
	{ok, events()} | {errror, Err} when Err :: term().

peek(Sel) ->
	peek(Sel, []).


-spec peek(sel(), peek_opts()) ->
	{ok, events()} | {errror, Err} when Err :: term().

peek({chan, Chan}, Opts) ->
	with_chan(Chan, fun
		(_Pid, []) -> {error, no_queue};
		(Pid, Queues) ->
			{ok, Items} = eredis:q(Pid, ["SUNION" | [key(Q, queue) || Q <- Queues]]),
			return_items(Items, Opts)
	end);
peek({queue, Queue}, Opts) ->
	{Pid, Cont} = get_pid(),
	{ok, Items} = eredis:q(Pid, ["SMEMBERS", key(Queue, queue)]),
	_ = Cont(Pid),
	return_items(Items, Opts).


-spec flush(channel()) ->
	{ok, events()} | {errror, Err} when Err :: term().

flush({chan, Chan}) ->
	with_chan(Chan, fun
		(_Pid, []) -> {error, no_queue};
		(Pid, Queues) ->
			{ok, Items} = eredis:q(Pid, ["SUNION" | [key(Q, queue) || Q <- Queues]]),
			[{ok, _}|_] = eredis:qp(Pid, [["SREM", Q | Items] || Q <- Queues]),
			{ok, Items}
	end).

-spec remove(channel(), event()) ->
	{ok, channel()} | {error, Err} when Err :: term().

remove({chan, Chan}, Event) ->
	with_chan(Chan, fun
		(_Pid, []) -> {error, no_queue};
		(Pid, Queues) ->
			[{ok, _}|_] = eredis:qp(Pid, [["SREM", key(Q, queue), Event] || Q <- Queues]),
			ok
	end).

-spec size({queue, queue()} | {chan, channel()}) ->
	{ok, non_neg_integer()} | {error, Err} when Err :: term().

size({chan, Chan}) ->
	with_chan(Chan, fun
		(_Pid, []) -> {error, no_queue};
		(Pid, Queues) ->
			Res = eredis:qp(Pid, [["SCARD", key(Q, queue)] || Q <- Queues]),
			lists:foldl(fun
				({ok, N}, {ok, Acc}) ->
					{ok, binary_to_integer(N) + Acc};

				({error, _} = Err, _) -> Err

			end, {ok, 0}, Res)
	end);
size({queue, Queue}) ->
	{Pid, Cont} = get_pid(),
	case eredis:q(Pid, ["SCARD", key(Queue, queue)]) of
		{ok, Size} ->
			_ = Cont(Pid),
			{ok, binary_to_integer(Size)};

		{error, _} = Err ->
			_ = Cont(Pid),
			Err
	end.

-spec meta({chan, channel()}, all | binary()) ->
	{ok, Val} | {error, Err} when Err :: term(),
	                              Val :: undefined | binary() | [tuple()].
meta({chan, Chan}, all) ->
	{Pid, Cont} = get_pid(),

	case eredis:q(Pid, ["HGETALL", key([Chan], chanmeta)]) of
		{ok, Items} ->
			_ = Cont(Pid),
			{ok, pair(Items)};

		{error, _} = Err ->
			_ = Cont(Pid),
			Err
	end;
meta({chan, Chan}, Key) ->
	{Pid, Cont} = get_pid(),
	Res = eredis:q(Pid, ["HGET", key([Chan], chanmeta), Key]), 
	_ = Cont(Pid),
	Res.

-spec meta({chan, channel()}, binary(), binary()) ->
	ok | {error, Err} when Err :: term().
meta({chan, Chan}, Key, Val) ->
	meta({chan, Chan}, Key, Val, set).

meta({chan, Chan}, Key, Val, inc) ->
	meta2({chan, Chan}, Key, Val, "HINCRBY");
meta({chan, Chan}, Key, Val, set) ->
	meta2({chan, Chan}, Key, Val, "HSET").

meta2({chan, Chan}, Key, Val, Op) ->
	{Pid, Cont} = get_pid(),
	Res = case eredis:q(Pid, [Op, key([Chan], chanmeta), Key, Val]) of
		{ok, _}          -> ok;
		{error, _} = Err -> Err
	end,
	_ = Cont(Pid),
	Res.

-spec consume({queue, queue()} | {chan, channel()}) ->
	{ok, channel()} | {error, Err} when Err :: term().

consume(Sel) ->
	consume(Sel, []).


-spec consume({queue, queue()} | {chan, channel()}, consume_opts()) ->
	{ok, channel()} | {error, Err} when Err :: term().

%% Creates a new channel for a queue
consume({queue, Resource}, Opts) ->
	Chan = fun(Pid) ->
		<<_:18/binary,N:32/integer, _/binary>> = term_to_binary(Pid),
		redq_chan:id(N)
	end,

	Queues = [Resource | proplists:get_all_values(queue, Opts)],
	consume2(Chan, [join(Q, <<$/>>) || Q <- Queues], Opts);

consume({chan, Chan}, Opts) when is_binary(Chan) ->
	Queues = proplists:get_all_values(queue, Opts),
	consume2(Chan, [join(Q, <<$/>>) || Q <- Queues], Opts).

consume2(Chan0, Queues, Opts) ->
	{Pid, Cont} = get_pid(),

	Chan = if is_function(Chan0) -> Chan0(Pid);
	          true -> Chan0 end,

	% Add queue <> channel mappings
	case Queues of
		[] ->
			ok;

		Queues ->
			Ops = lists:foldr(fun(Queue, Acc) ->
				[["SADD", key([Chan], chanmap), Queue],
				 ["SADD", key([Queue], queuemap), Chan] | Acc]
			end, [], Queues),

			[{ok, _}|_] = eredis:qp(Pid, Ops)
	end,

	_ = Cont(Pid),

	case maybe_add_proxy(Chan, Opts) of
		{ok, _} ->
			{ok, {chan, to_binary(Chan)}};

		{error, _} = Err ->
			Err
	end.

-spec destroy(channel()) -> ok | {error, Error} when Error :: term().

destroy({chan, Chan}) ->
	redq_chan:destroy(Chan).


%% Private
maybe_add_proxy(Chan, Opts) ->
	Parent = case lists:keyfind(proxy,1, Opts) of
		{proxy, Pid} -> Pid;
		false        -> lists:member(proxy, Opts) andalso self()
	end,

	Patterns = proplists:get_all_values(chan, Opts),

	if is_pid(Parent) -> redq_chan:new(Chan, Patterns, Parent, Opts);
	   true -> {ok, Chan}
	end.

get_pid() ->
	% make use of m:f/a style functions to use redq with pools
	{{M, F, A}, Return} = case application:get_env(redq, pool) of
		{ok, [{M1, F1, A1}, {M2, F2, A2}]} ->
			{{M1, F1, A1}, fun(P) -> erlang:apply(M2, F2, A2 ++ [P]) end};
		{ok, {M1, F1, A1}} ->
			{{M1, F1, A1}, fun(_) -> ok end} end,

	try erlang:apply(M, F, A) of
		{ok, P} ->
			{P, Return};

		P when is_pid(P) ->
			{P, Return};

		_ ->
			false
	catch
		A:B -> {error, {A, B}}
	end.

key(A, T) when is_list(A) ->
	join([rq, T | A], <<$/>>);
key(A, T) ->
	join([rq, T, A], <<$/>>).

join([], _Sep) ->
	<<>>;
join(Parts, Sep) ->
	<<Sep:1/binary, Acc/binary>> = join(Parts, Sep, <<>>),
	Acc.

join([], _Sep, Acc) ->
	Acc;
join([[] | Rest], Sep, Acc) ->
	join(Rest, Sep, Acc);
join([P | Rest], Sep, Acc) ->
	join(Rest, Sep, <<Acc/binary, Sep/binary, ((to_binary(P)))/binary>>).

to_binary(P) when is_list(P) -> iolist_to_binary(P);
to_binary(P) when is_atom(P) -> atom_to_binary(P, unicode);
to_binary(P) when is_integer(P) -> to_binary(integer_to_list(P));
to_binary(P) when is_binary(P) -> P.

with_chan(Chan, Fun) ->
	{Pid, Cont} = get_pid(),

	case eredis:q(Pid, ["SMEMBERS", key([Chan], chanmap)]) of
		{ok, undefined} ->
			_ = Cont(Pid),
			{error, notfound};

		{ok, Queues} ->
			Res = Fun(Pid, Queues),
			_ = Cont(Pid),
			Res;

		{error, _} = Err ->
			Err
	end.

return_items(Items, Opts) ->
	{Pos, Len} = case [lists:keyfind(X, 1, Opts) || X <- [n, slice]] of
		[false, {slice, all}] -> {1, length(Items)};
		[false, false]        -> {1, 1};
		[{n, N}, false]       -> {N, 1};
		[_, {slice, L}]       -> {1, L};
		[_, {slice, P, L}]    -> {P, L}  end,

	{ok, lists:sublist(lists:sort(Items), Pos, Len)}.

pair(Items) -> pair(Items, []).

pair([], Acc) -> Acc;
pair([K, V | Rest], Acc) ->
	pair(Rest, [{K, V} | Acc]).


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
setup_() ->
	[ok, ok] = [application:ensure_started(App) || App <- [eredis, redq]],
	ok = cleanerlmsg(),
	ok = cleanredis().

shutdown_(_) ->
	ok.
%%
%% There are 2 reasons to clean:
%% a) Since we resume the queue, we have to note that the `take/1`
%%    cannot remove the items from the erlang message queue. But you
%%    should not resume a queue created in the same process.
%% b) Push sends a message to two channels, the queue itself and
%%    volatile channel. this means everything gets delivered twice
%%
%% Sorry for the mess :)
cleanerlmsg() ->
	case receive {event, _, _} -> ok after 0 -> fail end of
		fail -> ok;
		ok -> cleanerlmsg()
	end.

cleanredis() ->
	{Pid, Cont} = get_pid(),
	{ok, _} = eredis:q(Pid, ["FLUSHALL"]),
	Cont(Pid).

simple_queue_consumer_test_() ->
	{setup
	, fun setup_/0
	, fun shutdown_/1
	, ?_test(begin
		Q = ["ns", "res", "child"],
		{ok, Chan} = redq:consume({queue, Q}),

		ok = redq:push(Chan, <<"ev">>),

		{ok, [<<"ev">>]} = redq:take(Chan),
		{ok, []} = redq:take(Chan),

		ok
	end)}.

multiplex_consumer_test_() ->
	{setup
	, fun setup_/0
	, fun shutdown_/1
	, ?_test(begin
		% Consume multiple queues
		{Q1, Q2} = {[<<"a">>, <<"b">>], [<<"a">>, <<"c">>]},
		{ok, Chan} = redq:consume({queue, Q1}, [{queue, Q2}]),

		Ev = <<"mjaumjau">>,
		?assertEqual(ok, redq:push(Chan, Ev)),

		?assertEqual({ok, [Ev]}, redq:peek(Chan)),
		?assertEqual({ok, [Ev]}, redq:take({queue, Q1})),
		?assertEqual({ok, [Ev]}, redq:take({queue, Q2})),
		?assertEqual({ok, []}, redq:take(Chan)),

		ok
	end)}.

named_consumer_test_() ->
	{setup
	, fun setup_/0
	, fun shutdown_/1
	, ?_test(begin
		Chan = {chan, <<"named-chan">>},
		Q = [<<"a">>, <<"b">>],
		Ev = <<"nnev">>,

		%% Create a named chan without any corresponding queues
		{ok, Chan} = redq:consume(Chan),
		?assertEqual(ok, redq:push(Chan, Ev)),

		%% Create a named chan with single queue connected
		{ok, Chan} = redq:consume(Chan, [{queue, Q}]),

		%% Add a queue to the channel
		?assertEqual(ok, redq:push(Chan, Ev)),

		?assertEqual({ok, [Ev]}, redq:peek(Chan)),
		?assertEqual({ok, [Ev]}, redq:take({queue, Q})),

		?assertEqual({ok, []}, redq:peek(Chan)),

		ok
	end)}.

consumers_test_() ->
	{setup
	, fun setup_/0
	, fun shutdown_/1
	, ?_test(begin
		Queue = {queue, [<<"a">>, <<"b">>, <<"c">>]},
		{E1, E2, E3} = {<<"e-1">>, <<"e-2">>, <<"e-3">>},

		{ok, {_, C} = Chan} = redq:consume(Queue, [proxy]),
		?assertEqual(ok, redq:push(Queue, E1)),

		?assertEqual({event, C, E1}
			, receive E -> E after 1000 -> timeout end),

		ok = redq:destroy(Chan),
		{ok, {_, C2} = Chan2} = redq:consume(Queue, [{channel, C}, proxy]),

		?assertEqual(ok, redq:push(Queue, E2)),
		?assertEqual(ok, redq:push(Queue, E3)),

		?assertEqual({ok, [E1]}, redq:peek(Chan2)),
		?assertEqual({ok, [E1]}, redq:take(Chan2)),

		ok = receive
			{event, C2, <<"e-2">>} = E ->
				?assertEqual({event, C2, <<"e-2">>}, E),
				?assertEqual(ok, redq:remove(Chan2, E2))
			after 1000 -> timeout end,

		?assertEqual({ok, [E3]}, redq:flush(Chan2)),

		?assertEqual(ok, redq:destroy(Chan2))
	end)}.


multi_consumer_test() ->
	{setup
	, fun setup_/0
	, fun shutdown_/1
	, ?_test(begin
		_ = [application:ensure_started(X) || X <- [eredis, redq]],

		Queue = [<<"d">>, <<"e">>, <<"f">>],
		E1 = <<"m-e-1">>,
		{ok, Q} = redq:consume(Queue),

		Parent = self(),
		lists:foreach(fun(N) ->
			sync_spawn(fun() ->
				Child = spawn_link(fun() ->
					receive
						{event, LQ, E} ->
							Parent ! {N, E, LQ},
							redq:destroy(LQ)
					end
				end),
				{ok, _} = redq:consume(Queue, [{proxy, Child}])

			end)
		end, Consumers = lists:seq(1, 35)),

		?assertEqual(ok, redq:push(Queue, E1)),

		getall(Consumers, E1),

		?assertEqual({ok, [E1]}, redq:take(Q, [])),

		%% We used consume/1, so no proxy process was created - destroy
		%% has no process to destroy so we get `notfound`
		?assertEqual({error, notfound}, redq:destroy(Q))
	end)}.

sync_spawn(Fun) ->
	Ref = make_ref(),
	Parent = self(),
	spawn(fun() ->
		Parent ! {Ref, Fun()}
	end),

	receive
		{Ref, Ret} -> Ret
	end.

getall([], _E) -> ok;
getall(Acc, E) ->
	receive {N, E, _Chan} ->
		getall(lists:delete(N, Acc), E)
	end.

% Support consuming multiple channels, currently only useful when
% using `proxy` option as there exists no channel -> channel
% relationship. The origin chan of the emitted event will be shadowed
% by the primary channel selected.
multi_channel_consumer_test_() ->
	{setup
	, fun setup_/0
	, fun shutdown_/1
	, ?_test(begin
		{ok, Chan} = redq:consume({chan, <<"a">>}, [{chan, <<"b">>}, proxy]),

		ok = redq:push({chan, <<"b">>}, <<"ev">>),

		?assertEqual({event, <<"b">>, <<"ev">>},
			receive {event, _, _} = E -> E
			after 1000 -> {error, timeout}
		end),

		{ok, Chan2} = redq:consume({chan, <<"c">>}, [{chan, <<"d">>}, rewrite, proxy]),
		ok = redq:push({chan, <<"d">>}, <<"ev">>),
		?assertEqual({event, <<"c">>, <<"ev">>},
			receive {event, _, _} = E -> E
			after 1000 -> {error, timeout}
		end)
	end)}.

size_test_() ->
	{setup
	, fun setup_/0
	, fun shutdown_/1
	, ?_test(begin
		{Q1, Q2} = {[<<"a">>, <<"b">>], [<<"a">>, <<"c">>]},
		{ok, Chan} = redq:consume({queue, Q1}, [{queue, Q2}]),

		?assertEqual(ok, redq:push({queue, Q1}, <<"ev1">>)),
		?assertEqual(ok, redq:push({queue, Q2}, <<"ev2">>)),
		?assertEqual(ok, redq:push({queue, Q2}, <<"ev3">>)),

		?assertEqual({ok, 1}, redq:size({queue, Q1})),
		?assertEqual({ok, 2}, redq:size({queue, Q2})),
		?assertEqual({ok, 3}, redq:size(Chan)),

		ok
	end)}.

meta_test_() ->
	{setup
	, fun setup_/0
	, fun shutdown_/1
	, ?_test(begin
		{Q1, Q2} = {[<<"a">>, <<"b">>], [<<"a">>, <<"c">>]},
		{ok, Chan} = redq:consume({queue, Q1}, [{queue, Q2}]),

		?assertEqual(ok, redq:meta(Chan, <<"k">>, <<"v">>)),
		?assertEqual({ok, <<"v">>}, redq:meta(Chan, <<"k">>)),
		?assertEqual({ok, undefined}, redq:meta(Chan, <<"not-found">>)),

		?assertEqual(ok, redq:meta(Chan, <<"x">>, <<"1">>)),
		?assertEqual(ok, redq:meta(Chan, <<"y">>, <<"2">>)),
		?assertEqual(ok, redq:meta(Chan, <<"z">>, <<"3">>)),

		?assertEqual({ok, [{<<"z">>, <<"3">>},
		                   {<<"y">>, <<"2">>},
		                   {<<"x">>, <<"1">>},
		                   {<<"k">>, <<"v">>}]}, redq:meta(Chan, all)),

		?assertEqual(ok, redq:meta(Chan, <<"inc">>, 1, inc)),
		?assertEqual(ok, redq:meta(Chan, <<"inc">>, 2, inc)),
		?assertEqual({ok, <<"3">>}, redq:meta(Chan, <<"inc">>)),

		ok
	end)}.

%tree_publish_test() ->
%	{setup
%	, fun setup_/0
%	, fun shutdown_/1
%	, ?_test(begin
%		_ = [application:ensure_started(X) || X <- [eredis, redq]],
%
%		P = self(),
%		lists:foldr(fun
%			(_, []) -> ok;
%			(_, [_ | NAcc] = Acc) ->
%			spawn(fun() ->
%				{ok, Q} = redq:consume(lists:reverse(Acc), [proxy]),
%				P ! {ok, length(Acc)},
%				receive {event, Q, _E} ->
%					P  ! {ok, length(Acc)},
%					redq:destroy(Q)
%				end
%			end),
%			NAcc
%		end, lists:reverse(["x", "y", "z", "x"]), lists:seq(1, 4)),
%
%		[receive {ok, N} -> ok end || N <- lists:seq(1,4)],
%
%		ok = redq:push(["x", "y", "z", "x"], "e", [volatile, broadcast]),
%
%		?assertEqual([ok,ok,ok],
%			[receive {ok, N} -> ok end || N <- lists:seq(2, 4)])
%	end)}.
%
-endif.

