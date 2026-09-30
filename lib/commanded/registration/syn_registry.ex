defmodule Commanded.Registration.SynRegistry do
  @moduledoc """
  Distributed process registration for Commanded on top of syn.

  Commanded uses its registry to make event handlers and process routers
  cluster singletons, and to find aggregate processes. syn registers a name
  with a write to a local table plus a broadcast and takes no cluster-wide
  lock, so a node that stops answering falls behind on its own view of the
  registry without holding up registrations anywhere else.

  ## Conflicts

  Two nodes that cannot see each other can each register the same name. syn
  detects the clash once they meet and asks
  `Commanded.Registration.SynRegistry.ConflictResolution` which process
  to keep: the one started first wins, and the other is stopped on its own
  node. An aggregate stops with `:normal`, and Commanded's dispatcher retries
  the command against the process that won. An event handler or process
  router stops with `{:shutdown, :name_conflict}`, and the process hosting
  it starts it again as a proxy for the process that won.

  ## One host process per singleton

  `start_link/5` starts a host process, internal to this adapter, for every
  event handler and process router, on every node, and Commanded supervises
  the host in the handler's place. The host runs the handler itself or a
  proxy for the process that holds the name, and it starts that child again
  only for registry churn: the handler lost its name in a conflict, or the
  proxy exited because the process holding the name went down, which also
  covers a name that still points at a process on a node that has gone
  away. A restart that syn refuses because the name still points at a local
  process that has already exited counts as churn too, and the host tries
  again until the name clears. A conflict round or a failover wave across a
  hundred handlers leaves the restart budget of the application's
  supervision tree alone. The host absorbs up to 30 such restarts within 5
  seconds and exits with `{:too_many_registry_restarts, name}` on the next
  one.

  Every other exit of the handler ends the host with the handler's own exit
  reason. A crash, a crash loop, or a stop that the handler's `error/3`
  callback asks for reaches the application's supervisor unchanged, and its
  restart strategy and budget apply as if it supervised the handler directly.

  The shutdown value the application's supervisor applies to the host also
  decides how long the handler gets to stop. The host passes the shutdown on
  to the handler and waits for it as long as the supervisor waits. When that
  wait runs out, or under `:brutal_kill`, the supervisor kills the host, and
  the handler is killed with it, even in the middle of a callback.

  ## Configuration

      registry: Commanded.Registration.SynRegistry

  or, to set the delay:

      registry: [adapter: Commanded.Registration.SynRegistry, failover_delay_range: {200, 1_000}]

  Each Commanded application gets its own syn scope, named after the
  application's name, which is the application module unless the
  application is started with a `name:` option. Two applications on one
  node therefore never share a name.

  `:failover_delay_range` bounds the random delay a proxy waits after the process
  holding the name goes down and before it exits itself, which is what makes
  its host start the child again and try for the name. The delay is
  drawn uniformly from those bounds and defaults to `{200, 1_000}`, so the
  proxies on all nodes reach that retry at different moments: the first one
  registers the name, and the rest find it taken and proxy the new holder.
  It also bounds the rate of the restart loop that runs while the name still
  points at a process that has exited on another node, because that node has
  gone away or because its scope process has not yet reported the exit.

  `:failover_delay_range` is the only option. Any other key, or the same key
  given twice, raises `ArgumentError` naming the application and the key when
  the Commanded application starts.

  ## Effects beyond Commanded, and known limitations

  Starting an application with this adapter installs
  `Commanded.Registration.SynRegistry.ConflictResolution` as syn's event
  handler for the whole node. It applies the rule above only in the scopes
  the adapter created, one per Commanded application, and leaves every other
  scope to the event handler installed before it, or to syn's own rule when
  there was none. Its module documentation covers what an event handler
  installed later has to do. The README section "Known limitations" covers
  the behaviour that is known to differ from Commanded's local registry.
  """

  @behaviour Commanded.Registration.Adapter

  require Logger

  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.SingletonHost
  alias Commanded.Registration.SynRegistry.SingletonProxy
  alias Commanded.Registration.SynRegistry.VanishedHolder

  @default_failover_delay_range {200, 1_000}

  # A singleton's reaper finds the processes its host started through the
  # `:parent` item of `Process.info/2`, which OTP 25 added, and the test
  # suite runs on OTP 26 to 29. The check runs while this module compiles,
  # so a project on an older release fails to build.
  @minimum_otp_release 26

  otp_release = System.otp_release()

  if String.to_integer(otp_release) < @minimum_otp_release do
    raise "commanded_syn_registry requires OTP #{@minimum_otp_release} or later, " <>
            "but this build runs on OTP #{otp_release}"
  end

  @doc """
  Adds the application's scope to this node and returns the adapter metadata.

  The scope is `application`, the name Commanded started the application
  under: its module, or the `name:` option it was started with. Before
  adding the scope, the adapter records it as its own and installs
  `Commanded.Registration.SynRegistry.ConflictResolution` as syn's event
  handler, which keeps the handler it replaces for every other scope. The
  adapter starts no process of its own and does not wait at boot: the node
  registers names as soon as the scope is added, and syn reconciles them
  with the rest of the cluster.

  Raises `ArgumentError` for an option other than `:failover_delay_range`,
  for an option given twice, and for a `:failover_delay_range` that is not a
  valid range.
  """
  @impl Commanded.Registration.Adapter
  def child_spec(application, config) do
    failover_delay_range =
      config
      |> validate_config!(application)
      |> Keyword.fetch!(:failover_delay_range)
      |> SingletonProxy.validate_failover_delay_range!()

    :ok = ConflictResolution.install_for_scope(application)
    :ok = :syn.add_node_to_scopes([application])

    {:ok, [],
     %{application: application, scope: application, failover_delay_range: failover_delay_range}}
  end

  @doc """
  Returns the child spec Commanded uses to start `module` as a supervisor.

  The spec starts `module` with `arg` and registers no name.
  """
  @impl Commanded.Registration.Adapter
  def supervisor_child_spec(_adapter_meta, module, arg) do
    %{id: module, start: {module, :start_link, [arg]}, type: :supervisor}
  end

  @doc """
  Starts a uniquely named child process of a `DynamicSupervisor`.

  This is the path aggregates take. The child registers itself under the syn
  name, so a start that loses the race returns the process that won it. When
  the winner is gone before it can be reported, the start is attempted again a
  few times, so the dispatch reaches a fresh aggregate.
  """
  @impl Commanded.Registration.Adapter
  def start_child(adapter_meta, name, supervisor, child_spec) do
    registration_name = build_registration_name(adapter_meta, name)
    spec = build_registered_child_spec(child_spec, registration_name)

    start =
      VanishedHolder.start_with_retry(fn -> DynamicSupervisor.start_child(supervisor, spec) end)

    case start do
      {:error, {:already_started, pid}} when is_pid(pid) -> {:ok, pid}
      reply -> reply
    end
  end

  @doc """
  Starts the process that hosts the singleton registered under `name`.

  The pid returned is the host's. The name resolves to the singleton the
  host runs, or to the process on another node that holds it.
  """
  @impl Commanded.Registration.Adapter
  def start_link(adapter_meta, name, module, args, start_opts) do
    SingletonHost.start_link(adapter_meta, name, module, args, start_opts)
  end

  @doc """
  Returns the pid registered under `name`, or `:undefined`.
  """
  @impl Commanded.Registration.Adapter
  def whereis_name(adapter_meta, name) do
    scope = Map.fetch!(adapter_meta, :scope)

    :syn.whereis_name({scope, name})
  end

  @doc """
  Returns a `:via` tuple routing messages to the process registered under
  `name`.
  """
  @impl Commanded.Registration.Adapter
  def via_tuple(adapter_meta, name) do
    scope = Map.fetch!(adapter_meta, :scope)

    {:via, :syn, {scope, name}}
  end

  # Fallbacks for `use Commanded.Registration`. They are not part of
  # `Commanded.Registration.Adapter`, but Commanded's event handlers,
  # aggregates and process routers call them for any `GenServer` message they
  # do not handle themselves.
  @doc false
  def handle_call(_request, _from, _state) do
    process = identify_process()

    raise "attempted to call GenServer #{inspect(process)} but no handle_call/3 clause was provided"
  end

  @doc false
  def handle_cast(_request, _state) do
    process = identify_process()

    raise "attempted to cast GenServer #{inspect(process)} but no handle_cast/2 clause was provided"
  end

  @doc false
  def handle_info(message, state) do
    log_unexpected_message(message)

    {:noreply, state}
  end

  # Commanded passes the `registry:` keyword without `:adapter`, so every key
  # left is an option of this adapter.
  defp validate_config!(config, application) do
    Keyword.validate!(config, failover_delay_range: @default_failover_delay_range)
  rescue
    error in ArgumentError ->
      message =
        "invalid :registry option for Commanded application #{inspect(application)}: " <>
          Exception.message(error)

      reraise ArgumentError, message, __STACKTRACE__
  end

  defp log_unexpected_message(message) do
    process = identify_process()

    [
      "SynRegistry: unexpected message:",
      "process=#{inspect(process)}",
      "message=#{inspect(message)}"
    ]
    |> Enum.join(" ")
    |> Logger.debug()
  end

  defp identify_process do
    case Process.info(self(), :registered_name) do
      {:registered_name, []} -> self()
      {:registered_name, name} -> name
    end
  end

  defp build_registered_child_spec(module, registration_name) when is_atom(module),
    do: {module, name: registration_name}

  defp build_registered_child_spec({module, args}, registration_name)
       when is_atom(module) and is_list(args),
       do: {module, Keyword.put(args, :name, registration_name)}

  # The registration carries the time the process is started and marks it as
  # an aggregate, which is what `ConflictResolution` reads when the same name
  # turns up on two nodes.
  defp build_registration_name(adapter_meta, name) do
    scope = Map.fetch!(adapter_meta, :scope)

    {:via, :syn, {scope, name, ConflictResolution.build_registration_metadata(:aggregate)}}
  end
end
