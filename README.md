# Commanded.Registration.SynRegistry

A process registry adapter for [Commanded](https://hexdocs.pm/commanded)
built on [syn](https://hexdocs.pm/syn), for Commanded applications that run
on several connected nodes.

Commanded uses its registry for two things: to run each event handler and
each process manager as a single process across the cluster, and to find the
process of an aggregate wherever it runs. This adapter keeps those names in
syn. A registration is a call to the local syn scope process, which writes
its table and broadcasts the entry to the other nodes; a lookup reads the
local table. Neither waits for another node.

The adapter also works on a single node.

## What the adapter does about clustering

Nothing beyond registration. It connects no nodes: use
[libcluster](https://hexdocs.pm/libcluster) or `Node.connect/1` for that.
syn runs as an application of its own, started as a dependency, and its scope
processes find each other over Erlang distribution when nodes connect. A node
joins the scope of a Commanded application when that application starts on
it, so only the nodes that run the application take part in its registry.

## Installation

```elixir
def deps do
  [
    {:commanded_syn_registry, "~> 0.1"}
  ]
end
```

The package needs Elixir 1.15 or later on OTP 26 or later, `commanded`
1.4.11 or later within 1.x, and `syn` 3.4.2 or later within 3.x.

## Configuration

Set the adapter as the registry of the Commanded application, in
`use Commanded.Application`:

```elixir
defmodule MyApp.CommandedApp do
  use Commanded.Application,
    otp_app: :my_app,
    event_store: [adapter: Commanded.EventStore.Adapters.EventStore, event_store: MyApp.EventStore],
    registry: Commanded.Registration.SynRegistry
end
```

or in the application environment, which Commanded merges over the options
given to `use`:

```elixir
config :my_app, MyApp.CommandedApp,
  registry: Commanded.Registration.SynRegistry
```

The keyword form takes options:

```elixir
registry: [adapter: Commanded.Registration.SynRegistry, failover_delay_range: {200, 1_000}]
```

`:failover_delay_range` is the only option. It bounds, in milliseconds, the
random delay a node waits before it tries to take over a singleton whose
process went down with its node, was stopped by it, was already gone, or
lost a name conflict (see [Failover](#failover)). The value is a
`{min_ms, max_ms}` tuple of non-negative integers with `min_ms <= max_ms`,
and the default is `{200, 1_000}`. When the Commanded application starts,
an invalid range raises `ArgumentError` naming the key, and a key the
adapter does not know or a key given twice raises `ArgumentError` naming
the application and the key.

The adapter keeps the application's names in a syn scope named after the
application's name, which is its module (`MyApp.CommandedApp` above) unless
you start the application with a `name:` option. It adds the node to that
scope when the application starts. Two Commanded applications on one node
get two scopes and never see each other's names.

## How it works

### Aggregates

Commanded starts an aggregate as a child of its `DynamicSupervisor` through
the adapter's `start_child/4`. The adapter rewrites the child spec so that
the aggregate registers itself under
`{:via, :syn, {scope, name, %{started_at: ...}}}`, where `started_at` is the
node's system time in nanoseconds at the start. When the name is already
held, on this node or on another one, syn refuses the registration and the
adapter returns the holder's pid, so the command goes to the process that
already runs the aggregate, over distribution when it runs elsewhere.
Aggregates are `:temporary` children: one that stops is not restarted, and
the next command for it starts a new one.

A holder can die between syn's refusal and the lookup that reports it, in
which case the lookup finds nobody. The adapter then waits 10 ms and starts
again, up to five times, because the entry behind the refusal is removed as
soon as the scope process handles the holder's exit. After the fifth refusal
the error is returned as it is.

### Event handlers and process managers

Commanded starts every event handler, and the router of every process
manager, on every node, under whatever supervisor you put them in. Only one
of these processes may hold the name. The adapter's `start_link/5` therefore
returns a host process of its own, one per handler and per node. The host
runs a single child and decides what it is each time it starts one:

- when the name is free, the child is the handler itself, registered under
  the syn name, again with `started_at` in the registration metadata;
- when the name is taken, the child is a proxy, which monitors the process
  holding the name and does nothing else. It is not registered under the
  name and forwards no messages. Anything that looks the name up gets the
  process on the hosting node.

A start that syn refuses for a holder that has already gone is retried the
same way as for aggregates.

The host starts its child again only for registry churn: the handler lost
its name in a conflict (see [Network partitions](#network-partitions)); syn
refused a restart, past the attempts above, because the name still points
at a process that has already exited; or the proxy saw the holder go down
with a reason that means its node is lost or stopping (`:noconnection`,
`:shutdown`, `:killed`), that the name pointed at a process that was
already gone (`:noproc`), or that the holder lost a conflict
(`{:shutdown, :name_conflict}`). Those restarts have a budget of their own;
see [Failover](#failover). Every other exit of the handler, a crash,
`:normal` or the `{:stop, reason}` its `error/3` callback asked for, ends
the host with the handler's exact reason on every node: on the node that
ran the handler, and on every node that proxied it, where the proxy passes
the reason on at once. The restart type and restart intensity of the
supervisor you put the handler under therefore apply on every node, as they
do with Commanded's `:global` adapter, with one exception: a handler that
stops with one of the five reasons above, `:shutdown`, `:killed`,
`:noproc`, `:noconnection` or `{:shutdown, :name_conflict}`, is read as
stopped by its node or moved by the registry. The parents on the other
nodes do not see that stop: those nodes proxy the handler's replacement
when one holds the name by the end of their failover delay, and take the
name over when none does. Stop a handler deliberately with
`{:stop, {:shutdown, reason}}`, where `reason` is anything but
`:name_conflict`, the adapter's own reason for a lost conflict; that stop
reaches every parent. See [Supervising handlers](#supervising-handlers).

A handler that fails to start at the first start makes `start_link/5`
return `{:error, {:shutdown, {:failed_to_start_child, module, reason}}}`;
at a restart the host ends with `reason` itself.

Process manager instances run under their router on the node that hosts it
and are not registered. An event handler started with `concurrency: n` is
`n` handler processes with `n` names, and each of them is a singleton of its
own.

The pid Commanded gets back from `start_link/5`, and keeps as the handler's
pid, is the host's. This matters in one place; see
[Known limitations](#known-limitations).

### Failover

When the process holding a name goes down with its node or is stopped by it
(`:noconnection`, `:shutdown`, `:killed`), and when the holder lost a
conflict (`{:shutdown, :name_conflict}`), every proxy for it receives the
monitor's `:DOWN` message, waits a delay drawn uniformly from
`:failover_delay_range`, and exits with the same reason the holder had. A
proxy started for a name that pointed at a process already gone gets
`:noproc` at once and spends the same delay looking the name up every
10 ms: when another process takes the name within the delay, the proxy
monitors that one instead, and otherwise it exits with `:noproc` as the
delay runs out. After either exit the host starts its child again, which
tries to register the name: the first node to get there hosts the handler,
and the nodes that arrive later find the name taken and proxy the new
holder. The delay spreads the attempts out in time, so most nodes find the
name taken instead of clashing over it. Two nodes that do register the same
name at once are sorted out the way a partition is (below), and the loser
becomes a proxy.

When the hosting node is lost rather than the process, the proxies get
`:noconnection`, and syn removes the departed node's names as soon as its
scope process is seen down. A restart that comes before that removal finds
the name still pointing at the dead process, starts a proxy for it, gets
`:noconnection` from the monitor at once, and tries again after another
delay. The same loop runs when the process dies on a node that stays up and
that node's scope process is slow to report the exit: until the report
arrives, the local table keeps the dead pid, and the proxy started for it
gets `:noproc` at once. This loop ends without a restart when the holder's
node registers a replacement within the delay: one of the proxy's lookups
finds it, and the proxy monitors it. Each host allows 30 restarts for
registry churn in 5 seconds, which is above the rate this loop reaches
with the default minimum delay of 200 ms. Delays that average under about
170 ms let the loop outrun the budget while the stale entry remains. Past
the budget the host logs an error and exits with
`{:too_many_registry_restarts, name}`, which its parent handles under its
own restart policy. A crash loop of the handler never touches this budget:
a crash ends the host with the crash reason, and the parent's restart
intensity decides.

A graceful stop of the hosting node, through `System.stop/1`,
`:init.stop/0` or the default handling of `SIGTERM`, stops its applications
before it closes its connections, so the proxies see the handler's
`:shutdown`, or `:killed` when the handler was still in a callback as the
supervisor's shutdown value ran out, and the name moves to another node
before the stopping node is gone. `:noconnection` means the node went away
without stopping: a crash of the VM, a kill of the OS process, or a lost
network.

Any other exit of the holder is its own: a crash, `:normal`, or the
`{:stop, reason}` its `error/3` asked for. The proxy passes it on at once,
without the delay, its host ends with that reason, and the parent on that
node applies its restart type, as the parent on the node that ran the
handler does. Each parent counts every exit of the handler while its node's
proxy monitors the current holder. After a passed-on exit, a node whose
table still names the dead holder catches up with the replacement within
about 10 ms of the replacement's registration reaching it, through the
lookups described above; a replacement that exits before that is not
counted on that node. A handler that fails on the same event after every
start is therefore restarted and counted by every parent, each over its own
`max_seconds`, the window in which a supervisor counts restarts.
`:permanent` parents with the same intensity that saw every exit run out of
it in the same round as long as the exits come well within that window. A
parent that missed an exit stops later than the others, if at all: it
keeps restarting the handler after they have stopped for as long as the
handler's exits then come too far apart to fill its window;
[Supervising handlers](#supervising-handlers) says how to keep that
contained.

The parent's shutdown value, a timeout, `:brutal_kill` or `:infinity`,
decides how long a handler gets to stop: the host passes the shutdown on to
the handler and waits with no limit of its own. When the parent kills the
host, the handler is killed with it, even while it is blocked in a callback
or still starting, and its name is released.

A takeover happens at least `min_ms` after the holder went down. A lower
range makes takeovers faster and clashes more likely; a higher range does
the opposite.

### Supervising handlers

Commanded builds every handler's child spec with `restart: :permanent`, and
the host is what that spec starts. A handler that keeps failing on one
event under `:permanent` parents is restarted by every node's parent until
their restart intensities run out, in the same round on every node apart
from the exits a node can miss (see [Failover](#failover)), and the exit
then climbs each supervision tree; on a tree with default intensities that
can stop the application on every node at about the same time. Commanded's
`:global` adapter behaves the same way. Two plain OTP settings keep it
contained.

Stop deliberately with `{:stop, {:shutdown, reason}}` from `error/3`, and
give the handler `restart: :transient`:

```elixir
children = [
  Supervisor.child_spec(MyApp.MyHandler, restart: :transient)
]
```

A `:transient` child that exits with `{:shutdown, reason}` is not
restarted: the supervisor keeps its spec with no process behind it, counts
nothing against its intensity and writes no supervisor report, and a
`GenServer` writes no crash report for that reason either. With this
adapter the proxies pass the reason on at once, so the same happens on
every node, and the handler stays stopped in the whole cluster after one
handling; `Supervisor.restart_child/2` brings it back by hand. Crashes
still restart, on every node. The reason has to be a `{:shutdown, term}`
tuple other than `{:shutdown, :name_conflict}`: the other nodes read a bare
`:shutdown`, `:killed`, `:noproc` or `:noconnection` as the holder's node
stopping it or as a stale registry entry, `{:shutdown, :name_conflict}` as
a lost conflict, and on any of the five the handler keeps running, started
again on its own node or taken over by another. `Supervisor.child_spec/2`
overrides the `restart` of a handler with `concurrency: 1`, the default;
with a higher concurrency Commanded starts a supervisor of its own whose
workers are `:permanent`, and the override reaches only that supervisor.

Put the handlers under a supervisor of their own, and make that supervisor
`:transient` under the application supervisor:

```elixir
children = [
  MyApp.CommandedApp,
  Supervisor.child_spec(MyApp.HandlerSupervisor, restart: :transient)
]
```

A supervisor that runs out of restart intensity exits with `:shutdown`.
Under `:transient` or `:temporary` its parent neither restarts it nor
counts the exit, so the handlers stop on every node while the Commanded
application keeps dispatching commands. `:temporary` never restarts the
supervisor; `:transient` also brings it back if something kills it.

This layout relies on the adapter's failover. With the `:global` adapter a
`:transient` handler whose node stops gracefully exits with `:shutdown` on
every node and is restarted nowhere.

### Network partitions

While two groups of nodes cannot see each other, each group runs its own
copy of every singleton and may start its own process for the same
aggregate, since neither side sees the other's registrations. When the nodes
reconnect, the syn scope processes exchange their registrations, and for
every name registered on both sides syn asks
`Commanded.Registration.SynRegistry.ConflictResolution` which process to
keep. It asks on both nodes that own a conflicting process, each on its own,
so the rule uses nothing but the two registrations: the one with the older
`started_at` wins, and equal timestamps go to the greater pid in Erlang term
order. Both nodes reach the same answer. `started_at` is the system time of
the node that started the process, so the comparison is only as good as the
clocks of the two nodes.

syn drops the loser's registration and leaves the process alive.
`ConflictResolution` then stops the loser on its own node with
`GenServer.stop/2`, from a process of its own, because the callback runs
inside the syn scope process, which has to go on serving registrations. The
stop reason follows the kind recorded in the registration: `:normal` for an
aggregate, `{:shutdown, :name_conflict}` for an event handler or process
router, and `:normal` for a registration whose metadata has no `:kind` key.
The stop is a system message, so a command or an event the loser is
handling at that moment, and anything already queued at it, completes first.

For an aggregate, the losing copy is not restarted. A caller whose command
reaches the losing copy after the stop sees the `:normal` exit as
`{:normal, :aggregate_stopped}` inside Commanded's dispatcher, which retries
the command: the retry starts the aggregate again, finds the name held by
the winner and sends the command there. The caller gets the usual result, or
`{:error, :too_many_attempts}` once the router's `retry_attempts` (10 by
default) are used up.

For an event handler or process router, the losing copy stops with
`{:shutdown, :name_conflict}`, its host starts the child again, and the
child finds the name taken and becomes a proxy for the winner. The adapter
decides only which process keeps the name. What each copy did with events
while the partition lasted is between Commanded and the event store
adapter; see
[Compared with the :global registry](#compared-with-the-global-registry)
for what the PostgreSQL event store does about it.

### Logs

The adapter logs at `:info` when a proxy fails over after its singleton
went down (`SingletonProxy: singleton down, stopping the proxy: name=...
node=... reason=... delay_ms=...`), when a proxy finds that the name
pointed at a process already gone (`SingletonProxy: name points at a
process that is gone, looking it up again: name=... node=...
delay_ms=...`) and then finds a new holder (`SingletonProxy: name taken by
a new process, proxying it: name=... node=... pid=...`), when a proxy
passes the holder's own exit on (`SingletonProxy: singleton exited on its
own, passing the exit on: name=... node=... reason=...`) and when a process
loses a conflict (`SynRegistry: conflict lost, stopping the process:
scope=... name=... pid=...`). A host that runs out of its restart budget
logs at `:error` (`SingletonHost: too many registry restarts, stopping
the host: name=... max_restarts=30 window_ms=5000`). Each restart for
registry churn, and a message a Commanded process has no clause for, is
logged at `:debug`. syn itself logs scope discovery, node arrivals and
departures and each conflict at `:notice`.

## What the adapter changes for the whole node

Starting a Commanded application with this adapter calls
`:syn.set_event_handler/1` with
`Commanded.Registration.SynRegistry.ConflictResolution`. syn keeps one event
handler per node, in the application environment of the `:syn` application,
so the module is installed for every syn scope on the node. It applies the
rule described under [Network partitions](#network-partitions) only in the
scopes the adapter created, one per Commanded application, and leaves every
other scope as it was:

- if a handler was configured before the first Commanded application
  started, with `config :syn, event_handler: ...` or
  `:syn.set_event_handler/1`, every callback from such a scope goes to that
  handler when it exports the callback. This includes the process group
  callbacks;
- without one, syn's own rule decides a conflict: the registration made last
  wins, equal registration times go to the greater pid, and the loser is
  sent the exit signal `{:syn_resolve_kill, name, metadata}` by its own
  node.

One difference remains: an exception raised by the earlier handler is caught
and logged by syn, and the log names `ConflictResolution` as the callback
module.

A host that installs its own handler after a Commanded application has
started replaces the adapter's, in every scope. The adapter keeps working
when that handler passes the callbacks of the adapter's scopes back to it.
`ConflictResolution.adapter_scope?/1` tells those scopes apart:

```elixir
defmodule MyApp.SynEventHandler do
  @behaviour :syn_event_handler

  alias Commanded.Registration.SynRegistry.ConflictResolution

  @impl :syn_event_handler
  def resolve_registry_conflict(scope, name, entry, other_entry) do
    if ConflictResolution.adapter_scope?(scope),
      do: ConflictResolution.resolve_registry_conflict(scope, name, entry, other_entry),
      else: resolve_host_conflict(scope, name, entry, other_entry)
  end

  @impl :syn_event_handler
  def on_process_unregistered(scope, name, pid, metadata, reason) do
    if ConflictResolution.adapter_scope?(scope),
      do: ConflictResolution.on_process_unregistered(scope, name, pid, metadata, reason),
      else: :ok
  end
end
```

The handler has to export both callbacks: syn kills the loser of a conflict
itself when the installed handler does not export
`resolve_registry_conflict/4`. The adapter needs no other callback.

## Known limitations

### Strong consistency and a handler that dispatches to its own application

With Commanded 1.4.11 and earlier, an event handler with
`consistency: :strong` that dispatches a command with `consistency: :strong`
to its own application from `handle/2` gets `{:error, :consistency_timeout}`
after the dispatch consistency timeout (5 seconds by default).

Commanded registers the pid returned by the registry adapter's
`start_link/5` as the handler's subscription. With this adapter that pid is
the handler's host. After a `:strong` dispatch,
`Commanded.Middleware.ConsistencyGuarantee` waits until every `:strong`
subscription has acknowledged the new events, excluding the dispatching
process by pid. The handler dispatches as itself, its pid never matches the
host's, so it waits for its own acknowledgement, which cannot arrive
before `handle/2` returns.

[commanded/commanded#668](https://github.com/commanded/commanded/pull/668)
fixes this by also excluding the dispatching handler by name. Until it is
released, dispatch with `consistency: :eventual` from such a handler. The
adapter changes nothing else about strong consistency: Commanded uses the
registered pid for this exclusion only and tracks acknowledgements by
handler name.

### syn 3.4.2 can stall on a repeated registry snapshot

In syn 3.4.2, a scope process that receives a second registry snapshot from
a node it already knows reconciles it in time that grows with the square of
that node's number of names. Until the reconciliation ends, lookups keep
working, but a registration or unregistration in the scope waits up to
syn's 5 s call timeout and then fails. A singleton that starts or restarts
in that time fails to start, and its parent restarts it about every 5
seconds, each attempt leaving a crash report (which Elixir's Logger shows
only with `handle_sasl_reports: true`); a `:temporary` parent drops it. A
dispatch that has to start an aggregate fails instead of waiting. The
trigger is rare, most often a node that connects while a scope process is
starting or restarting, but at around 100,000 names in a scope the stall
lasts minutes.
[ostinelli/syn#90](https://github.com/ostinelli/syn/pull/90) fixes it. Until
it is released, keep scopes small: by default an aggregate process keeps its
name for as long as it runs, and an aggregate lifespan
(`Commanded.Aggregates.AggregateLifespan`) that stops idle aggregates
releases their names.

## Running the tests

```sh
mix deps.get
mix test
```

`test/test_helper.exs` runs `epmd -daemon` and, unless the VM is already
distributed, turns it into a node named
`commanded_syn_registry_test@127.0.0.1`, so `epmd` has to be on the `PATH`
and that node name has to be free. The cluster tests start two peer nodes
with `:peer`, bound to `127.0.0.1` and started with `-connect_all false`, so
that each test decides when the nodes meet. CI runs the suite on Elixir 1.15
to 1.20 with OTP 26 to 29, together with `mix compile --warnings-as-errors`,
`mix format --check-formatted` and `mix credo --strict`; one entry of the
matrix also runs `mix dialyzer`, `mix docs` and `mix hex.build`.

## License

MIT. See the `LICENSE` file.

## Compared with the :global registry

Commanded ships an adapter on `:global`, the name registry in OTP's kernel.
What follows compares the two registries, and one choice the adapters
around them make differently. This adapter was written after the situations
below came up in a production cluster.

### Where :global hurts

`:global` makes a registration a transaction across the cluster: the
registering node takes a lock on every node it knows, sends the name to
every node and waits for each reply, then releases the lock. When the call
returns, the name is registered on all nodes or on none. That is what makes
the registry consistent, and each of the situations below follows from it.

A node that is frozen but still connected, one whose VM no longer runs while
its distribution connection stays up, answers nothing. Every lock round
includes it and waits for its reply, so registrations stop on every node in
the cluster, until the connection to the frozen node is dropped. For a node
that sends nothing, the distribution tick does that: with the default
`net_ticktime` of 60 seconds, between 45 and 75 seconds after the node went
quiet.

Every registration takes the same lock, so registrations run one at a time
across the whole cluster, each one a round of messages to every node. A
rolling deploy starts a node with all of its event handlers and process
managers at once, and each of those registrations waits for the ones ahead
of it, on that node and on the others.

After a partition heals, `:global` finds the names registered on both sides
and, with its default resolve function, keeps one process and kills the
other with an exit signal the process cannot trap. Whatever that process was
doing at the moment is lost.

`:global` also keeps the cluster fully connected. With
`prevent_overlapping_partitions` enabled, which it is by default, a node that
loses its connection to one other node reports the loss to the rest, and
every other node disconnects from both of them. A connection lost between
two nodes therefore leaves both nodes outside the cluster. The OTP source
says itself that this takes down more connections than the minimum needed to
form fully connected partitions.

Commanded's `:global` adapter links the supervisor on every node to the one
process holding the name, so every exit of that process reaches every
supervisor. That is right for a crash and for a stop the handler asked for,
and this adapter keeps it. It is wrong for a node that stops or is lost:
every handler on that node exits at once, every other node's supervisor
counts one restart per handler, and four handlers are enough to exhaust the
default intensity of 3 restarts in 5 seconds and take the supervisor down
on the surviving nodes. Under `:transient` a graceful stop of one node ends
a handler everywhere instead. This adapter absorbs those exits in the host
and moves the name.

### Where syn is weaker

A syn registration is a write to the local table plus a broadcast, and a
node learns of a name registered elsewhere when the broadcast arrives.
Registrations are eventually consistent, so two nodes can register the same
name in that window and both starts succeed.

For the same reason, a node that joins or a partition that heals can briefly
run two copies of a singleton, and two processes for the same aggregate. The
adapter resolves that after the fact, as described under
[Network partitions](#network-partitions), and the copies run side by side
until then.

One scope process per node serves all names of that scope. Every
registration and unregistration in the scope goes through it, so anything
that keeps it busy, such as the snapshot reconciliation in
[Known limitations](#known-limitations), delays all of them. Lookups read
the tables directly and are not affected.

The syn event handler is one per VM. The adapter installs its own and routes
by scope, which limits the impact, but a host application with a handler of
its own has to delegate the adapter's scopes as shown under
[What the adapter changes for the whole node](#what-the-adapter-changes-for-the-whole-node).

Until commanded#668 is released, a strongly consistent handler that
dispatches a strongly consistent command to its own application times out;
see [Known limitations](#known-limitations). Commanded's `:global` adapter
returns the handler's own pid and does not have this problem.

### What the PostgreSQL event store does about duplicates

Two copies of a singleton, or of an aggregate, only matter if both act. With
Commanded's adapter for the PostgreSQL EventStore, they mostly cannot:

- Every subscription takes a PostgreSQL advisory lock keyed by the
  subscription, with `pg_try_advisory_lock`. Event handlers and process
  manager routers subscribe under their name, so a duplicate handler or
  router on another node fails to take the lock, receives no events, and
  tries the lock again after the subscription retry interval, one minute by
  default (`:subscription_retry_interval`). It gets events once the original
  is gone and its lock is released.
- An aggregate appends its events with the stream version it expects. When
  two copies of an aggregate run, the copy that writes second gets
  `{:error, :wrong_expected_version}`. Commanded's aggregate then rebuilds
  its state from the stream and retries the command against that state, up
  to the router's `retry_attempts` (10 by default), and returns
  `{:error, :too_many_attempts}` after that.

Other event store adapters may differ on both points.
