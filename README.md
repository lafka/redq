# REDQ

Lightweight persistent queue system using Redis and Erlang.

Features:
+ Hierarchical subscription
+ Use pointers to resume previous connection
+ Multiple consumers on same data

**WARNING** Highly experimental, docs/examples may not work 100%
or even aligned with the current API. Use at own risk.

### Overview

`chan` -> A channel operating on top of a resource
`queue` -> A resource containing set of events

`<ns>:queue:<resource>` -> set containing messages to be consumed
`<ns>:channels`            -> Hash defining channel -> queue lookup

**Push:**
When pushing an event the following happens:
+ add new element to set `<ns>:queue:<resource>`
+ publish `event` in channels: `<ns>:queue:<resource>`

**Peek:**
+ return all members in `<ns>:chan[<chan>]` and perform slicing

**Take:**
+ call `remove` for each element in return from `peek` call

**Consume:**
+ Maybe: remove `<ns>:channels[<prev-chan>]`
+ set `<ns>:channels[<new-chan>] = <resource>`
+ subscribe to `<ns>:queue:<resource>` (supervised process if `proxy` is used)

**Destroy:**
+ When chansumer was called with `proxy`, the supervised process
  running on local node will be terminated.


### Example

**Note:** Considering the code is alpha these examples may not work as
expected or even work at all. Refer to tests in `redq.erl` for working
examples.

```erlang
Resource = [<<"root">>, <<"group-1">>, <<"resource">>],
%% Push a message, if queue doest not exist it will be created
ok = redq:push(Resource, <<"event-1">>),

%% Subscribe to a specific resource, use `receive` option to get items
%% as messages.
{ok, Q} = redq:consume(Resource, [proxy]),
% => {ok, <<"Dh7c8VHZhH9642l8gW0">>}

%% Receive a single event without removing it
receive {event, Q, Event} -> {recv, Event} end,
% => {recv, <<"event-1">>}

ok = redq:push(Resource, <<"event-2">>),
ok = redq:push(Resource, <<"event-3">>),

%% Close consumer; this only closes redis connection, no data will be
%% removed data and additional data may be added to the queue
ok = redq:destroy(Q),

%% Take over from last position, creates new identifier Q2
{ok, Q2} = redq:consume(Resource, [{resume, Q}, proxy]),
% => {ok, <<"Dh7dGJabdgAKMcW8EAW">>}

% See head of queue without removing it
{ok, _Item} = redq:peek(Q2),
% => {ok, [<<"event-1">>]}

% Retreive and remove the head of the queue, returns {ok, []} if queue
% is empty
{ok, [<<"event-1">>]} = redq:take(Q2),

receive
	{event, Q2, AnotherEvent} ->
		%% We use `remove/2` to remove item from queue
		ok = redq:remove(Q2, AnotherEvent),
		{recv, AnotherEvent}
end,
% => {recv, <<"event-2">>}

%% Give us all the members and remove them from queue
{ok, [<<"event-3">>]} = redq:flush(Q2).


%% Let multiple people consume the queue
{ok, Q3} = redq:consume(Resource),
ok = redq:push(Resource, <<"event-4">>),

Parent = self(),
lists:foreach(fun(N) ->
	spawn(function() {
		{ok, LocalQ} = redq:consume(Resource, [proxy]),
		Parent ! N,

		receive
			{event, LocalQ, LocalEvent} ->
				io:format("consumer ~b <- ~s received event: ~s~n"
					, [N, LocalQ, LocalEvent])
		end
	})
end, Consumers = lists:seq(1, 10)),

[receive N -> ok || N <- Consumers],

%% Let's wait until everyone else have been sent a message, this does
%% not guarantee that all consumer have processed the item, but it
%% does ensure that all consumers have been created.

{ok, [Event]} = redq:take(Q3, [takelast]),
io:format("consumer -1 <- ~s took event: ~s~n", [Q3, Event]),

%% The above will result in 11 messages which will appear in random
%% order

%% Destroy the queue, removing all subscriptions
ok = redq:destroy(Q3).


%% We can broadcast events and observe them from any of the ancestors resources
ParentRes = lists:sublist(Resource, 2),
{ok, ParentQ} = redq:consume(ParentRes),
{ok, RootQ} = redq:consume(hd(Resource)),

ok = redq:push(Resource, <<"event-5">>, [broadcast]),
ok = redq:push(Resource, <<"event-6">>, [broadcast]),
ok = redq:push(Resource, <<"event-7">>, [broadcast]),

{ok, [<<"event-5">>]} = redq:peek(ParentQ),
{ok, [<<"event-5">>]} = redq:peek(RootQ),

%% Or even at a specific position, remember this is 0-based indexes
{ok, [<<"event-7">>]} = redq:peek(RootQ, [{n, 2}]),

%% Using `broadcast` one can observe all child queues, resulting in
%% different views.
ok = redq:push(Resource, <<"aaa-bbb">>, [broadcast]),

%% Want to slice the queue into pieces and put them in a box?
{ok, Box} = redq:take(RootQ, [{slice, 0, 3}),
% {ok, [<<"event-5">>, <<"event-6">>, <<"event-7">>, <<"aaa-bbb">>]}

%% Remember that I never gave you any strong consistency guarantees,
%% meaning multiple consumers may receive a event even though you use
%% `take/1`.
```
