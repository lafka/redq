# REDQ

Lightweight persistent queue system using Redis and Erlang.

Features:
+ Hierarchical subscription
+ Use pointers to resume previous connection
+ Multiple consumers on same data

**WARNING** Highly experimental, docs/examples are not fully working
or even aligned with the current API. Use at own risk.

### Overview

`chan` -> A channel operating on top of a resource
`queue` -> A resource containing set of events

`<ns>:queue:<resource>` -> set containing messages to be consumed
`<ns>:chans`            -> Hash defining channel -> queue lookup

**Push:**
When pushing an event the following happens:
+ add new element to set `<ns>:queue:<resource>`
+ publish `event` in channels: `<ns>:queue:<resource>`

**Peek:**
+ return all members in `<ns>:chan[<chan>]` and perform slicing

**Take:**
+ call `remove` for each element in return from `peek` call

**Consume:**
+ Maybe: remove `<ns>:chans[<prev-chan>]`
+ set `<ns>:chans[<new-chan>] = <resource>`
+ subscribe to `<ns>:queue:<resource>`

### Example

```erlang
Resource = [<<"root">>, <<"group-1">>, <<"resource">>],
%% Push a message, if queue doest not exist it will be created
ok = redq:push(Resource, <<"event-1">>),

%% Subscribe to a specific resource, use `receive` option to get items
%% as messages.
{ok, Q} = redq:consume(Resource, [proxy]),
% {ok, <<"Dh7c8VHZhH9642l8gW0">>}

%% Receive a single event without removing it
receive {event, Q, Event} -> {recv, Event} end,
% {recv, <<"event-1">>}

ok = redq:push(Resource, <<"event-2">>),
ok = redq:push(Resource, <<"event-3">>),

%% Close consumer; this only closes redis connection, no data will be
%% removed data and additional data may be added to the queue
ok = redq:close(Q),

%% Take over from last position, creates new identifier Q2
{ok, Q2} = redq:consume(Resource, [{resume, Q}]),
% {ok, <<"Dh7dGJabdgAKMcW8EAW">>}

% See head of queue without removing it
{ok, _Item} = redq:peek(Q2),
% {ok, [<<"event-1">>]}

% Retreive and remove the head of the queue, blocks until an element
% have been pushed to queue.
{ok, [<<"event-1">>]} = redq:take(Q2),

receive
	{event, Q2, AnotherEvent} ->
		%% We use `remove/2` to remove item from queue
		ok = redq:remove(Q2, AnotherEvent),
		{recv, AnotherEvent}
end,
% {recv, <<"event-2">>}

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
%% not guarantee that all consumer have processed the item, instead
%% it guarantees that the message have been sent to the consumer

{ok, [Event]} = redq:take(Q3, [takelast]),
io:format("consumer -1 <- ~s took event: ~s~n", [Q3, Event]),

%% The above will result in 11 messages which will appear in random
%% order

%% Destroy the queue, removing all subscriptions
ok = redq:purge(Q3).


%% We can observe the events from any of the ancestors resources
ParentRes = lists:sublist(Resource, 2),
{ok, ParentQ} = redq:consume(ParentRes),
{ok, RootQ} = redq:consume(hd(Resource)),

ok = redq:push(Resource, <<"event-5">>),
ok = redq:push(Resource, <<"event-6">>),
ok = redq:push(Resource, <<"event-7">>),

{ok, [<<"event-5">>]} = redq:peek(ParentQ),
{ok, [<<"event-5">>]} = redq:peek(RootQ),

%% Or even at a specific position, remember this is 0-based indexes
{ok, [<<"event-7">>]} = redq:peek(RootQ, [{n, 2}]),

%% Parent queues aggregates all child queues, resulting in different
%% views of the queue. Everything is lexical sorted so you might not
%% loose all your hair
ok = redq:push(Resource, <<"aaa-bbb">>),
{ok, [<<"aaa-bbb">>]} = redq:peek(RootQ),

%% Want to slice the queue into pieces and put them in a box?
{ok, Box} = redq:take(RootQ, [{slice, 0, 3}),
% {ok, [<<"aaa-bbb">>, <<"event-5">>, <<"event-6">>, <<"event-7">>]}

%% Remember that I never gave you any strong consistency guarantees,
%% meaning multiple consumers may receive a event even though you use
%% `take/1`.


%% Finally you can push events with a ttl, if the event is not taken
%% within the timeout it will be purged from redis and an notification
%% will be sent to the consumer:
{ok, Q4} = redq:consume(Resource),
ok = redq:push(Resource, <<"ttl-event">>, [{ttl, 10}]),

receive
	{expired, Q4, Resource} ->
			{expired, Resource}
end.

```
