-module(redq_chan).

-export([
	  new/3, new/4
	, consume/4
	, destroy/1
	, id/1
]).

-define(sup, redq_sup).

new(Chan, AddChans, Opts) ->
	new(Chan, AddChans, self(), Opts).

new(Chan, AddChans, Parent, Opts) ->
	supervisor:start_child(?sup, {{?MODULE, Chan}
		, {?MODULE, consume, [Chan, AddChans, Parent, Opts]}
		, permanent
		, 5000
		, worker
		, [?MODULE]}).

consume(Chan, AddChans, Parent, Opts) ->
	Pid = spawn_link(fun() ->
		{ok, KillFun} = add_subscription([Chan | AddChans]),

		not lists:member(nopeek, Opts) andalso
			case redq:peek(Chan, [{slice, all}]) of
				{ok, Items} ->
					_ = [Parent ! {event, Chan, E} || E <- Items];

				_ ->
					ok
			end,

		loop(Chan, Parent, KillFun)
	end),

	{ok, Pid}.

destroy(Chan) ->
	case lists:keyfind({redq_chan, Chan}, 1, supervisor:which_children(?sup)) of
		{{redq_chan, _} = ID, Pid, _Type, _} ->
			Ref = erlang:monitor(process, Pid),
			Pid ! stop,
			receive {'DOWN', Ref, process, _, _} ->
				ok = supervisor:delete_child(?sup, ID)
			end;

		false ->
			{error, notfound}
	end.

id(NS) ->
	random:seed(os:timestamp()),
	join([NS,
		integer_to_list(random:uniform(16#FFFFFFFFFFFFFFFFFFFFF), 36)]
		, <<$/>>).


add_subscription(Channels) ->
	Wildcard = nomatch =/= binary:match(iolist_to_binary(Channels), <<$*>>),
	{Sub, Cont} = get_sub_pid(),

	ok = eredis_sub:controlling_process(Sub),

	Cont2 = if
		Wildcard ->
			ok = eredis_sub:psubscribe(Sub, Channels),
			fun(P) ->
				eredis_sub:punsubscribe(P, Channels), Cont(P) end;

		true ->
			ok = eredis_sub:subscribe(Sub, Channels),
			fun(P) ->
				eredis_sub:unsubscribe(P, Channels), Cont(P) end
	end,

	[receive {subscribed, K, Sub} -> eredis_sub:ack_message(Sub) end
		|| K <- Channels],

	{ok, Cont2}.

loop(Chan, Proxy, CSP) ->
	receive
		{message, _Queue, Event, Pid2} ->
			ok = eredis_sub:ack_message(Pid2),

			Proxy ! {event, Chan, Event},

			loop(Chan, Proxy, CSP);

		{pmessage, _Pattern, _Queue, Event, Pid2} ->
			ok = eredis_sub:ack_message(Pid2),

			Proxy ! {event, Chan, Event},

			loop(Chan, Proxy, CSP);

		stop ->
			ok = supervisor:terminate_child(?sup, {?MODULE, Chan})
	end.

get_sub_pid() ->
	{{M, F, A}, Return} = case application:get_env(redq, pool_sub) of
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

to_binary(P) when is_list(P) -> iolist_to_binary(P);
to_binary(P) when is_atom(P) -> atom_to_binary(P, unicode);
to_binary(P) when is_integer(P) -> to_binary(integer_to_list(P));
to_binary(P) when is_binary(P) -> P.

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
