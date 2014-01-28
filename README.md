# REDQ

Lightweight persistent queue system using Redis and Erlang.

Features:
+ Persistent storage to feed transient subscribers
+ Multiple consumers on same data
+ Consume multiple feeds at once

**WARNING** Highly experimental, docs/examples will not work or be
aligned with the current API. Use at own risk.

## Terminology

### Queues

A queue is a defined as a path either in the form ["a", "b", "c"]
or in it's native form <<"a/b/c">>.

Queues are stored in Redis under the key `<ns>/queue/<q>`

### Channels
A channel is subscriber consuming a set of queues. The queues are the
the persistence, while the channel provides a volatile one-to-many
mapping the queues.

Channels are stored in two places, in the set `rq/chanmap/<chan> -> <queue>, ..`
and the reverse in the set `rq:queuemap:<queue> -> <chanid>, ..`

### Events

Events are defined as binary blobs, without any id - in fact a common
pattern is to publish resource id's through redq. An event is pushed
to a queue where it persists, then a PUBLISH is sent all the channels
associated with that queue.


## Overview

`chan`  -> A channel operating on top of a set of queues
`queue` -> A resource containing set of events

`rq/queue/<q>`    -> set containing persisted events
`rq/queuemap/<q>` -> set of `q` -> `chan` relations
`rq/chanmap/<q>` -> set of `chan` -> `q` relations

**Push:**
When pushing an event the following happens:
+ add new element to set `rq/queue/<q>`
+ publish `event` in channels defined in `rq/quemap/<q>`

**Peek:**
+ return all events defined by selector

**Take:**
+ call `remove` for each element in return from `peek` call

**Consume:**
+ If the selector is a queue create a new channel `chan`
+ subscribe to `chan` (supervised process if `proxy` is used)

**Destroy:**
+ When `consume` was called with `proxy`, the supervised process
  running on local node will be terminated.


### Example

**Note:** Considering the code is alpha these examples may not work as
expected or even work at all. Refer to tests in `redq.erl` for working
examples.

**Consume a queue by polling**:
```erlang
{ok, Chan} = redq:consume({queue, ["queue", "branch"]}),
% -> {ok, {chan, <<"ZnqsdyFufVNM">>}}

ok = redq:push(Chan, <<"queue-event">>),
{ok, [<<"queue-event">>]} = redq:peek(Chan), % Peek into the queue 
{ok, [<<"queue-event">>]} = redq:take(Chan), % Take all elements of the queue
{ok, []} = redq:peek(Chan),                  % Now it's empty

ok = redq:destroy(Chan).
```

**Consume a queue by proxy**:
```erlang
Opts = [proxy],
{ok, {chan, C} = Chan} = redq:consume({queue, ["branch", "twig"]}, Opts),

ok = redq:push(Chan, <<"event">>),

receive {event, C, E} -> io:format("got event ~p from ~p", [E, C]) end.
```


**Named channel consuming multiple queues**:
```erlang
% We can consume more queues by specifying them as options
Opts = [Q1 = {queue, ["branch", "leaf"]}, Q2 = {queue, ["branch2", "leaf"]}],

{ok, Chan} = redq:consume({chan, <<"named-chan">>}, Opts),
% -> {ok, {chan, <<"mZ5QqsuUoVFi">>}}

ok = redq:push(B1, <<"e1">>),
ok = redq:push(B2, <<"e2">>),

{ok, [<<"b1">>]} = redq:peek(Q1),             % The events exists individually
{ok, [<<"b2">>]} = redq:peek(Q2),             % in the separate queues
{ok, [<<"b1">>, <<"b2">>]} = redq:peek(Chan). % or combined in the chan
```

## TODO

+ add redq:stop/1 and make redq:destroy/1 remove the channel permanently
 + redq:destroy/1 must clean up after itself
+ Add selector type arguments to all functions
+ Fix tree broadcasting, meaning notify all children of the tree

