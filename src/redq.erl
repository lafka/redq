-module(redq).

-export([
	  push/2, push/3
	, consume/1, consume/2
	, destroy/1
	, peek/1, peek/2
	, take/1, take/2
	, flush/1
	, remove/2
]).

-type queue() :: [namespace() | binary()].
-type namespace() :: binary().
-type channel() :: binary().
-type event() :: binary().
-type events() :: [event()].

-type push_opts() :: [{ttl, non_neg_integer()}].
-type consume_opts() :: [{resume, channel()}].
-type take_opts() :: peek_opts().
-type peek_opts() :: [{slice, Pos :: non_neg_integer(), Len :: non_neg_integer()}
	| {slice, Len :: non_neg_integer()}
	| {n, Pos :: non_neg_integer()}].

-export_type([channel/0, event/0]).

-spec push(queue(), event()) ->
	ok | {error, Err} when Err :: term().

push(Queue, Event) ->
	push(Queue, Event, []).


-spec push(queue(), event(), push_opts()) ->
	ok | {error, Err} when Err :: term().

push([NS | Resource], Event, Opts) ->
	Queues = case lists:member(broadcast, Opts) of
		true ->
			Root0 = key(queue, NS, <<>>),
			Root  = binary:part(Root0, 0, size(Root0) - 1),
			lists:foldl(fun(A, [B|_] = Acc) ->
				[join([B, A], <<$:>>) | Acc]
			end, [Root], Resource);

		false ->
			[key(queue, NS, join(Resource, <<$:>>))] end,

	Expand = case lists:member(volatile, Opts) of
		true  -> fun(Q, Acc) -> [["PUBLISH", Q, Event] | Acc] end;
		false -> fun(Q, Acc) -> [["SADD", Q, Event], ["PUBLISH", Q, Event] | Acc] end
	end,

	{Pid, Cont} = get_pid(),
	[{ok, _} | _] = eredis:qp(Pid, lists:foldl(Expand, [], Queues)),

	_ = Cont(Pid),

	ok.


-spec take(channel()) ->
	{ok, events()} | {error, Err} when Err :: term().

take(Chan) ->
	take(Chan, []).


-spec take(channel(), take_opts()) ->
	{ok, events()} | {error, Err} when Err :: term().

take(Chan, Opts) ->
	[NS, _Chan] = binary:split(Chan, <<$/>>),

	%% @todo 2013-09-09; lafka - validate that items are removed
	with_queue([NS, Chan], fun(Pid, Queue) ->
		{ok, Items0} = eredis:q(Pid, ["SMEMBERS", key(queue, NS, Queue)]),
		{ok, Items} = return_items(Items0, Opts),
		_ = eredis:qp(Pid, [ ["SREM", key(queue, NS, Queue), I] || I <- Items]),
		{ok, Items}
	end).

-spec peek(queue()) ->
	{ok, events()} | {errror, Err} when Err :: term().

peek(Queue) ->
	peek(Queue, []).


-spec peek(queue(), peek_opts()) ->
	{ok, events()} | {errror, Err} when Err :: term().

peek(Chan, Opts) ->
	[NS, _Chan] = binary:split(Chan, <<$/>>),

	with_queue([NS, Chan], fun(Pid, Queue) ->
		{ok, Items} = eredis:q(Pid, ["SMEMBERS", key(queue, NS, Queue)]),
		return_items(Items, Opts)
	end).


-spec flush(channel()) ->
	{ok, events()} | {errror, Err} when Err :: term().

flush(Chan) ->
	[NS, _Chan] = binary:split(Chan, <<$/>>),

	with_queue([NS, Chan], fun(Pid, Queue) ->
		{ok, Items} = eredis:q(Pid, ["SMEMBERS", key(queue, NS, Queue)]),
		_ = eredis:qp(Pid, [ ["SREM", key(queue, NS, Queue), I] || I <- Items]),
		{ok, Items}
	end).


-spec remove(channel(), event()) ->
	{ok, channel()} | {error, Err} when Err :: term().

remove(Chan, Event) ->
	[NS, _Chan] = binary:split(Chan, <<$/>>),

	with_queue([NS, Chan], fun(Pid, Queue) ->
		{ok, _} = eredis:q(Pid, ["SREM", key(queue, NS, Queue), Event]),
		ok
	end).


-spec consume(queue()) ->
	{ok, channel()} | {error, Err} when Err :: term().

consume(Queue) ->
	consume(Queue, []).


-spec consume(queue(), consume_opts()) ->
	{ok, channel()} | {error, Err} when Err :: term().

consume([NS | Resource], Opts) ->
	{Queue, Chan} = {join(Resource, <<$:>>), redq_chan:id(NS)},

	Transaction = case lists:keyfind(resume, 1, Opts) of
			{resume, OldChan} ->
				[["HDEL", key(channels, NS, []), OldChan]];

			false ->
				[] end,

	{Pid, Cont} = get_pid(),
	eredis:qp(Pid, [
		["HSET", key(channels, NS, []), Chan, Queue] | Transaction]),

	_ = Cont(Pid),

	maybe_add_proxy(Chan, [key(queue, NS, Queue)], Opts),

	{ok, to_binary(Chan)}.

-spec destroy(channel()) -> ok | {error, Error} when Error :: term().

destroy(Chan) ->
	redq_chan:destroy(Chan).


%% Private
maybe_add_proxy(Chan, AChans, Opts) ->
	Parent = case lists:keyfind(proxy,1, Opts) of
		{proxy, Pid} ->
			Pid;

		false ->
			lists:member(proxy, Opts) andalso self()
	end,


	if is_pid(Parent) ->
			redq_chan:new(Chan, AChans, Parent, Opts);

		true ->
			false
	end.

get_pid() ->
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

key(channels, Root, _) ->
	<<(to_binary(Root))/binary, ":channels">>;
key(queue, Root, Queue)    ->
	<<(to_binary(Root))/binary, ":queue:", (to_binary(Queue))/binary>>.

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

with_queue([NS, Chan], Fun) ->
	{Pid, Cont} = get_pid(),
	case eredis:q(Pid, ["HGET", key(channels, NS, []), Chan]) of
		{ok, undefined} ->
			_ = Cont(Pid),
			{error, notfound};

		{ok, Queue} ->
			_ = Cont(Pid),
			Fun(Pid, Queue)
	end.

return_items(Items, Opts) ->
	{Pos, Len} = case [lists:keyfind(X, 1, Opts) || X <- [n, slice]] of
		[false, {slice, all}] -> {1, length(Items)};
		[false, false]        -> {1, 1};
		[{n, N}, false]       -> {N, 1};
		[_, {slice, L}]       -> {1, L};
		[_, {slice, P, L}]    -> {P, L}  end,

	{ok, lists:sublist(lists:sort(Items), Pos, Len)}.


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
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

consumer_test() ->
	P = self(),
	spawn(fun() ->
		_ = [application:ensure_started(X) || X <- [eredis, redq]],

		Queue = [<<"a">>, <<"b">>, <<"c">>],
		{E1, E2, E3} = {<<"e-1">>, <<"e-2">>, <<"e-3">>},

		{ok, Q} = redq:consume(Queue, [proxy]),
		?assertEqual(ok, redq:push(Queue, E1)),

		?assertEqual({event, Q, E1}
			, receive E -> E after 1000 -> timeout end),

		{ok, Q2} = redq:consume(Queue, [{resume, Q}, proxy]),

		?assertEqual(ok, redq:push(Queue, E2)),
		?assertEqual(ok, redq:push(Queue, E3)),

		?assertEqual({ok, [E1]}, redq:peek(Q2)),
		?assertEqual({ok, [E1]}, redq:take(Q2)),

		receive {event, Q2, <<"e-2">>} = E ->
			?assertEqual({event, Q2, <<"e-2">>}, E),
			?assertEqual(ok, redq:remove(Q2, E2))
		end,

		?assertEqual({ok, [E3]}, redq:flush(Q2)),

		?assertEqual(ok, redq:destroy(Q2)),

		P ! ok
	end),

	receive ok -> ok end.


consume_wildcard_test() ->
	P = self(),
	spawn(fun() ->
		_ = [application:ensure_started(X) || X <- [eredis, redq]],

		Queue = [<<"a">>, <<"*">>],
		{ok, Q} = redq:consume(Queue, [proxy]),

		?assertEqual(ok, redq:push([<<"a">>, <<"b">>], <<"c">>)),
		receive {event, _, _} = E -> ?assertEqual(E, {event, Q, <<"c">>}) end,

		?assertEqual(ok, redq:destroy(Q)),

		P ! ok
	end),

	receive ok -> ok end.

multi_consumer_test() ->
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
	?assertEqual({error, notfound}, redq:destroy(Q)).

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

tree_publish_test() ->
	_ = [application:ensure_started(X) || X <- [eredis, redq]],

	cleanerlmsg(),

	P = self(),
	lists:foldr(fun
		(_, []) -> ok;
		(_, [_ | NAcc] = Acc) ->
		spawn(fun() ->
			{ok, Q} = redq:consume(lists:reverse(Acc), [proxy]),
			P ! {ok, length(Acc)},
			receive {event, Q, _E} ->
				P  ! {ok, length(Acc)},
				redq:destroy(Q)
			end
		end),
		NAcc
	end, lists:reverse(["x", "y", "z", "x"]), lists:seq(1, 4)),

	[receive {ok, N} -> ok end || N <- lists:seq(1,4)],

	ok = redq:push(["x", "y", "z", "x"], "e", [volatile, broadcast]),

	?assertEqual([ok,ok,ok],
		[receive {ok, N} -> ok end || N <- lists:seq(2, 4)]).

-endif.

