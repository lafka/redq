-module(redq).

-export([
	  push/2, push/3
	, consume/1, consume/2
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
	Queue = join(Resource, <<$:>>),
	Key = key(queue, NS, Queue),

	{Pid, Cont} = get_pid(),

	case lists:member(volatile, Opts) of
		true ->
			{ok, _} = eredis:q(Pid, ["PUBLISH", Key, Event]);

		false ->
			[{ok, _}, {ok, _}] = eredis:qp(Pid, [["SADD", Key, Event]
				, ["PUBLISH", Key, Event]])
	end,

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
	{Queue, Chan} = {join(Resource, <<$:>>), gen_chan_id(NS)},

	lists:member(proxy, Opts) andalso begin
		Parent = self(),
		spawn(fun() ->
			{ok, SubPid, Cont} = add_subscription([Chan, key(queue, NS, Queue)]),

			_ = create_pid_monitor(Parent, SubPid, Cont),

			case peek(Chan, [{slice, all}]) of
				{ok, Items} ->
					_ = [Parent ! {event, Chan, E} || E <- Items];
				_ ->
					ok
			end,

			consumer(SubPid, Chan, Parent)
		end)
	end,

	Transaction = case lists:keyfind(resume, 1, Opts) of
			{resume, OldChan} ->
				[["HDEL", key(channels, NS, []), OldChan]];

			false ->
				[] end,

	{Pid, Cont} = get_pid(),
	eredis:qp(Pid, [
		["HSET", key(channels, NS, []), Chan, Queue] | Transaction]),
	_ = Cont(Pid),

	{ok, to_binary(Chan)}.


consumer(Pid, Chan, Proxy) ->
	receive {message, _Queue, Event, Pid2} ->
		ok = eredis_sub:ack_message(Pid2),

		is_pid(Proxy) andalso (Proxy ! {event, Chan, Event}),

		consumer(Pid, Chan, Proxy)
	end.

add_subscription(Channels) ->
	Wildcard = nomatch =/= binary:match(iolist_to_binary(Channels), <<$*>>),
	{Sub, Cont} = get_sub_pid(),

	ok = eredis_sub:controlling_process(Sub),

	Cont2 = if
		Wildcard ->
			ok = eredis_sub:psubscribe(Sub, Channels),
			fun(P) -> eredis_sub:punsubscribe(P, Channels), Cont(P) end;

		true ->
			ok = eredis_sub:subscribe(Sub, Channels),
			fun(P) -> eredis_sub:unsubscribe(P, Channels), Cont(P) end
	end,

	[receive {subscribed, K, Sub} -> eredis_sub:ack_message(Sub) end
		|| K <- Channels],

	{ok, Sub, Cont2}.


%% Private
get_pid() ->
	get_pid2(redq, pool).

get_sub_pid() ->
	get_pid2(redq, pool_sub).

get_pid2(App, Key) ->
	{{M, F, A}, Return} = case application:get_env(App, Key) of
		{ok, [{M1, F1, A1}, {M2, F2, A2}]} ->
			{{M1, F1, A1}, fun(P) -> erlang:apply(M2, F2, A2 ++ [P]) end};
		{ok, {M1, F1, A1}} ->
			{{M1, F1, A1}, fun(_) -> ok end} end,

	case erlang:apply(M, F, A) of
		{ok, P} -> {P, Return};
		P when is_pid(P) -> {P, Return}
	end.

create_pid_monitor(Parent, Pid, Cont) ->
	spawn(fun() ->
		Ref = erlang:monitor(process, Parent),
		receive {'DOWN', Ref, _, _, _} ->
				Cont(Pid)
		end
	end).

gen_chan_id(NS) ->
	random:seed(os:timestamp()),
	join([NS,
		integer_to_list(random:uniform(16#FFFFFFFFFFFFFFFFFFFFF), 36)]
		, <<$/>>).

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

to_binary(P) when is_list(P) -> list_to_binary(P);
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

consumer_test() ->
	_ = application:load(redq),

	Queue = [<<"a">>, <<"b">>, <<"c">>],
	{E1, E2, E3} = {<<"e-1">>, <<"e-2">>, <<"e-3">>},

	{ok, Q} = redq:consume(Queue, [proxy]),
	?assertEqual(ok, redq:push(Queue, E1)),

	?assertEqual({event, Q, E1}
		, receive E -> E after 1000 -> timeout end),

	?assertEqual(ok, redq:push(Queue, E2)),
	?assertEqual(ok, redq:push(Queue, E3)),

	%?assertEqual(ok, redq:close(Q)),

	{ok, Q2} = redq:consume(Queue, [{resume, Q}, proxy]),

	?assertEqual({ok, [E1]}, redq:peek(Q2)),
	?assertEqual({ok, [E1]}, redq:take(Q2)),

	%% First event is transmitted multiple times since take cannot
	%% remove items from message queue, therefor it's important to
	%% only rely on 'proxy' or 'take'
	receive {event, Q2, <<"e-2">>} = E ->
		?assertEqual({event, Q2, <<"e-2">>}, E),
		?assertEqual(ok, redq:remove(Q2, E2))
	end,

	?assertEqual({ok, [E3]}, redq:flush(Q2)).

multi_consumer_test() ->
	_ = application:load(redq),

	Queue = [<<"d">>, <<"e">>, <<"f">>],
	E1 = <<"m-e-1">>,
	{ok, Q} = redq:consume(Queue),

	Parent = self(),
	lists:foreach(fun(N) ->
		spawn_link(fun() ->
			{ok, LQ} = redq:consume(Queue, [proxy]),
			Parent ! N,
			receive {event, LQ, E} -> Parent ! {N, E} end
		end)
	end, Consumers = lists:seq(1, 250)),


	?assertEqual(ok, redq:push(Queue, E1)),
	Resp  = [receive {N, E1} -> ok after 200 -> {error, N} end || N <- Consumers],
	?assertEqual([ok || _ <- Consumers], Resp),

	?assertEqual({ok, [E1]}, redq:take(Q, [])).

-endif.
