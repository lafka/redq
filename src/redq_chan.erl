-module(redq_chan).

-export([
	  new/3, new/4
	, consume/4
	, id/1
]).

-define(sup, redq_chan_sup).

new(Chan, Patterns, Opts) ->
	new(Chan, Patterns, self(), Opts).

new(Chan, Patterns, Parent, Opts) ->
	supervisor:start_child(?sup, [Chan, Patterns, Parent, Opts]).

consume(Chan, Patterns, Parent, Opts) ->
	Self = self(),
	Pid = spawn_link(fun() ->
		{ok, KillFun} = add_subscription([Chan | Patterns]),

		% Proxy old events if there are any events in the channel
		not lists:member(nopeek, Opts) andalso
			case redq:peek({chan, Chan}, [{slice, all}]) of
				{ok, Items} ->
					_ = [Parent ! {event, Chan, E} || E <- Items];

				_ ->
					ok
			end,

		Alive = is_process_alive(Parent),
		if
			Alive ->
				Ref = monitor(process, Parent),
				Self ! {ok, Ref},
				loop(Chan, Parent, KillFun, lists:member(rewrite, Opts));

			true ->
				% Don't bother restarting if parent is dead, parent
				% must respawn new consumer
				Self ! {error, parent}
		end
	end),

	receive
		{ok, _Ref} -> {ok, Pid};
		{error, parent} -> {error, dead_parent}
	after 5000 -> {error, timeout} end.

id(Worker) ->
	{M, S, Ms} = erlang:now(),
	<<I:80>> = <<Worker:16, ((M*100000000 + S*1000000 + Ms)):64>>,
	as_bin(I, 62).

%%
%% n.b. - unique_id_62/0 and friends pulled from riak
%%
as_bin(I, 10) ->
	erlang:integer_to_binary(I);
as_bin(I, Base) when is_integer(I), is_integer(Base),
	Base >= 2, Base =< 1+$Z-$A+10+1+$z-$a ->

	list_to_binary(if I < 0 -> [$-|as_bin(-I, Base, [])];
	   true -> as_bin(I, Base, [])
	end);
as_bin(I, Base) ->
	erlang:error(badarg, [I, Base]).

as_bin(I0, Base, R0) ->
	{D, I1} = {I0 rem Base, I0 div Base},
	R1 = if D >= 36 -> [D-36+$a|R0];
			D >= 10 -> [D-10+$A|R0];
			true -> [D+$0|R0]
	end,

	if I1 =:= 0 -> R1;
	   true -> as_bin(I1, Base, R1)
	end.


add_subscription(Channels) ->
	Wildcard = nomatch =/= binary:match(iolist_to_binary(Channels), <<$*>>),
	{Sub, Cont} = get_sub_pid(),

	ok = eredis_sub:controlling_process(Sub),

	Cont2 = if
		Wildcard ->
			ok = eredis_sub:psubscribe(Sub, Channels),
			fun() ->
				eredis_sub:punsubscribe(Sub, Channels), Cont(Sub) end;

		true ->
			ok = eredis_sub:subscribe(Sub, Channels),
			fun() ->
				eredis_sub:unsubscribe(Sub, Channels), Cont(Sub) end
	end,

	[receive {subscribed, K, Sub} -> eredis_sub:ack_message(Sub) end
		|| K <- Channels],

	ok = eredis_sub:controlling_process(Sub),

	{ok, Cont2}.

% If this crashes we will be with CSP never been called and
% subscription never being cleaned up. Maybe spawn a separate process
% to monitor instead....
loop(Chan, Proxy, CSP, Rewrite) ->
	receive
		{message, SrcChan, Event, Pid2} ->
			ok = eredis_sub:ack_message(Pid2),

			if Rewrite -> Proxy ! {event, Chan, Event};
			   true -> Proxy ! {event, SrcChan, Event} end,

			loop(Chan, Proxy, CSP, Rewrite);

		{pmessage, _Pattern, SrcChan, Event, Pid2} ->
			ok = eredis_sub:ack_message(Pid2),

			if Rewrite -> Proxy ! {event, Chan, Event};
			   true -> Proxy ! {event, SrcChan, Event} end,

			loop(Chan, Proxy, CSP, Rewrite);

		{'DOWN', _Ref, process, _Pid, _Reason} ->
			CSP(),
			redq_chan_manager:destroy(Chan);

		stop ->
			CSP(),
			ok
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

		Err ->
			error_logger:error_msg("redis subscription failed: ~p~n", [Err]),
			false
	catch
		A:B -> {error, {A, B}}
	end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup_() ->
	_ = [ok = application:ensure_started(X) || X <- [eredis, redq]].

teardown_(_) ->
	_ = [ok = application:stop(X) || X <- [eredis, redq]].

pubsub_test() ->
	{setup
	, fun setup_/0
	, fun teardown_/1
	, ?_test(begin
	{ok, Pid} = eredis:start_link(),
	{ok, Chan} = redq_chan:new(<<"a">>, [], self(), []),
	{ok, _} = eredis:q(Pid, ["PUBLISH", "a", "xyz"]),

	receive {event, _, _} = X ->
		?assertEqual({event, <<"a">>, <<"xyz">>}, X) end,

	redq_chan:destroy(Chan)
	end)}.

% Check that all consumers are properly terminated when parent dies
kill_consumers_test_() ->
	{setup
	, fun setup_/0
	, fun teardown_/1
	, ?_test(begin
		Consumers = lists:seq(48, 51),
		Parent = self(),
		Children = [spawn(fun() ->
				{ok, P} = redq_chan:new(<<N>>, [], self(), []),
				Parent ! {N, P},
				receive terminate -> ok end
			end) || N <- Consumers],

		[receive {N, _P} -> ok end || N <- Consumers],

		?assertEqual(length(Children), length(supervisor:which_children(?sup))),
		[begin
			Ref = monitor(process, P),
			P ! terminate,
			receive {'DOWN', Ref, process, _, R} -> ok end
		 end || P <- Children],

		timer:sleep(10),

		?assertEqual([], supervisor:which_children(?sup))
	end)}.
-endif.
