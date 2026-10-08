defmodule Commanded.Registration.SynRegistry.ClusterTestNode do
  @moduledoc """
  The node side of the registry's cluster tests.

  `:peer.call/4` reaches only code that exists as a `.beam` file on the peer's
  code path, so everything the peer nodes run lives here instead of in the
  test file. Every function is called from the test process on the other end
  of the peer connection.

  Processes are unlinked from the caller because `:peer.call/4` runs each call
  in a process of its own, which exits as soon as the call returns.
  """

  alias Commanded.Registration.SynRegistry
  alias Commanded.Registration.SynRegistry.ClusterTestApplication
  alias Commanded.Registration.SynRegistry.ClusterTestQuickRejectingSingleton
  alias Commanded.Registration.SynRegistry.ClusterTestRejectingSingleton
  alias Commanded.Registration.SynRegistry.ClusterTestSingleton
  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.RecordingEventHandler
  alias Commanded.Registration.SynRegistry.SingletonHost
  alias Commanded.Registration.SynRegistry.SingletonProxy
  alias Commanded.Registration.SynRegistry.SingletonReaper

  @singleton_application :cluster_test_application

  @doc """
  Starts syn on this node and installs `event_handler` as its event handler,
  the way a host application does before any Commanded application starts.
  """
  @spec install_event_handler(event_handler :: module()) :: :ok
  def install_event_handler(event_handler) do
    {:ok, _started} = Application.ensure_all_started(:syn)

    :syn.set_event_handler(event_handler)
  end

  @doc """
  Starts the process that keeps what `RecordingEventHandler` reports on this
  node, for `recorded?/1` to read.
  """
  @spec start_recording() :: :ok
  def start_recording do
    recorder = spawn(__MODULE__, :record_callbacks, [])
    true = Process.register(recorder, RecordingEventHandler)

    :ok
  end

  @doc false
  @spec record_callbacks() :: no_return()
  def record_callbacks do
    receive do
      callback ->
        callbacks = :persistent_term.get({__MODULE__, :callbacks}, [])
        :persistent_term.put({__MODULE__, :callbacks}, [callback | callbacks])
        record_callbacks()
    end
  end

  @doc """
  Returns whether `RecordingEventHandler` has reported `callback` on this
  node.
  """
  @spec recorded?(callback :: tuple()) :: boolean()
  def recorded?(callback), do: callback in :persistent_term.get({__MODULE__, :callbacks}, [])

  @doc """
  Starts a plain process and registers it under `name` with `metadata` in
  `scope`, a scope of the host application that the adapter did not create.
  """
  @spec register_in_host_scope(scope :: atom(), name :: term(), metadata :: term()) :: pid()
  def register_in_host_scope(scope, name, metadata) do
    :ok = :syn.add_node_to_scopes([scope])
    pid = spawn(Process, :sleep, [:infinity])
    :ok = :syn.register(scope, name, pid, metadata)

    pid
  end

  @doc """
  Tells the syn event handler on this node that `loser`, which may run on
  another node, lost `name` in a conflict in `scope`, as syn does on every
  node that learns of the loss.
  """
  @spec report_conflict_loss(scope :: atom(), name :: term(), loser :: pid(), metadata :: term()) :: term()
  def report_conflict_loss(scope, name, loser, metadata) do
    ConflictResolution.on_process_unregistered(scope, name, loser, metadata, :syn_conflict_resolution)
  end

  @doc """
  Returns the process this node resolves `name` to in `scope`.
  """
  @spec whereis_in_scope(scope :: atom(), name :: term()) :: pid() | :undefined
  def whereis_in_scope(scope, name), do: :syn.whereis_name({scope, name})

  @doc """
  Starts syn on this node, adds the application's scope and returns the
  adapter metadata the other functions here take.
  """
  @spec start_registry(application :: module(), SingletonProxy.failover_delay_range()) :: SingletonHost.adapter_meta()
  def start_registry(application, failover_delay_range) do
    {:ok, _started} = Application.ensure_all_started(:syn)

    {:ok, [], adapter_meta} = SynRegistry.child_spec(application, failover_delay_range: failover_delay_range)

    adapter_meta
  end

  @doc """
  Starts a singleton for `name` and returns its host process.
  """
  @spec start_singleton(SingletonHost.adapter_meta(), name :: term()) :: host :: pid()
  def start_singleton(adapter_meta, name) do
    {:ok, host} = SynRegistry.start_link(adapter_meta, name, ClusterTestSingleton, :state, [])

    Process.unlink(host)

    host
  end

  @doc """
  Starts a supervisor whose one child is the host of a `singleton_module`
  singleton for `name`, and returns the supervisor. `child_overrides` go into
  the host's child spec, and `supervisor_options` to `Supervisor.start_link/2`.
  """
  @spec start_supervised_singleton(
          SingletonHost.adapter_meta(),
          name :: term(),
          singleton_module :: module(),
          child_overrides :: keyword(),
          supervisor_options :: keyword()
        ) :: supervisor :: pid()
  def start_supervised_singleton(adapter_meta, name, singleton_module, child_overrides, supervisor_options) do
    host_spec = build_host_spec(adapter_meta, name, singleton_module, child_overrides)
    {:ok, supervisor} = Supervisor.start_link([host_spec], supervisor_options)

    Process.unlink(supervisor)

    supervisor
  end

  @doc """
  Loads and starts an application on this node whose supervisor hosts a
  `ClusterTestSingleton` for `name`, and returns that supervisor.

  The application starts after syn and depends on it, so a graceful stop of
  the node stops the singleton's host before syn and before the node's
  connections close.
  """
  @spec start_singleton_application(SingletonHost.adapter_meta(), name :: term()) :: supervisor :: pid()
  def start_singleton_application(adapter_meta, name) do
    application_keys = [mod: {ClusterTestApplication, {adapter_meta, name}}, applications: [:kernel, :stdlib, :syn]]
    :ok = :application.load({:application, @singleton_application, application_keys})
    :ok = :application.start(@singleton_application)
    {:ok, supervisor} = :application.get_supervisor(@singleton_application)

    supervisor
  end

  @doc """
  Returns the child spec of a host for a `singleton_module` singleton
  registered under `name`, with `child_overrides` applied.
  """
  @spec build_host_spec(
          SingletonHost.adapter_meta(),
          name :: term(),
          singleton_module :: module(),
          child_overrides :: keyword()
        ) :: Supervisor.child_spec()
  def build_host_spec(adapter_meta, name, singleton_module, child_overrides) do
    host_start = {SynRegistry, :start_link, [adapter_meta, name, singleton_module, :state, []]}

    Supervisor.child_spec(%{id: :singleton, start: host_start}, child_overrides)
  end

  @doc """
  Starts a process for `name` the way an aggregate is started, under a
  `DynamicSupervisor` of this node, and returns the process.
  """
  @spec start_aggregate(SingletonHost.adapter_meta(), name :: term()) :: pid()
  def start_aggregate(adapter_meta, name) do
    {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    Process.unlink(supervisor)

    {:ok, pid} = SynRegistry.start_child(adapter_meta, name, supervisor, {ClusterTestSingleton, []})

    pid
  end

  @doc """
  Returns the child of a singleton's host process, or of the supervisor
  above a host, and what it is: the registered process itself, a proxy for
  the node holding the name, or a host.
  """
  @spec fetch_child(supervisor :: pid()) :: {child :: pid(), :singleton | :proxy | :host | :unknown} | :restarting
  def fetch_child(supervisor) do
    case Supervisor.which_children(supervisor) do
      [{_id, pid, _type, _modules}] when is_pid(pid) -> {pid, classify_child(pid)}
      _children -> :restarting
    end
  end

  @doc """
  Returns the process this node resolves `name` to.
  """
  @spec whereis(SingletonHost.adapter_meta(), name :: term()) :: pid() | :undefined
  def whereis(adapter_meta, name), do: SynRegistry.whereis_name(adapter_meta, name)

  @doc """
  Starts watching `pid` so that `fetch_exit_reason/1` can report how it
  ended.
  """
  @spec watch_exit(pid()) :: :ok
  def watch_exit(pid) do
    spawn(__MODULE__, :record_exit, [pid])

    :ok
  end

  @doc false
  @spec record_exit(pid()) :: :ok
  def record_exit(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, reason} -> :persistent_term.put({__MODULE__, pid}, reason)
    end
  end

  @doc """
  Returns how a watched process ended, or `nil` while it is still running.
  """
  @spec fetch_exit_reason(pid()) :: term()
  def fetch_exit_reason(pid), do: :persistent_term.get({__MODULE__, pid}, nil)

  @doc """
  Starts a reaper for a stand-in host that runs no singleton, kills the host,
  and returns the pid of the reaper. `fetch_exit_reason/1` tells how the
  reaper ended once `watch_exit/1` was called on it.
  """
  @spec start_reaper_of_killed_host(scope :: atom(), name :: term()) :: pid()
  def start_reaper_of_killed_host(scope, name) do
    host = spawn(__MODULE__, :run_stand_in_host, [self(), scope, name])

    receive do
      {:reaper, reaper} ->
        :ok = watch_exit(reaper)
        Process.exit(host, :kill)

        reaper
    end
  end

  @doc false
  @spec run_stand_in_host(caller :: pid(), scope :: atom(), name :: term()) :: no_return()
  def run_stand_in_host(caller, scope, name) do
    {reaper, _reaper_ref} = SingletonReaper.start_monitor(scope, name)
    send(caller, {:reaper, reaper})

    Process.sleep(:infinity)
  end

  @doc """
  Holds syn's registry process for `scope` on this node from now until
  `delay_ms` after `holder` exits, the way a registry process busy with
  other names would. Until then this node neither reports that exit to the
  other nodes nor registers anything.
  """
  @spec hold_registry_past_exit(scope :: atom(), holder :: pid(), delay_ms :: non_neg_integer()) :: :ok
  def hold_registry_past_exit(scope, holder, delay_ms) do
    registry_process = :syn_backbone.get_process_name({:syn_registry, scope})
    :ok = :sys.suspend(registry_process)
    spawn(__MODULE__, :resume_registry_after_exit, [registry_process, holder, delay_ms])

    :ok
  end

  @doc false
  @spec resume_registry_after_exit(registry_process :: atom(), holder :: pid(), delay_ms :: non_neg_integer()) :: :ok
  def resume_registry_after_exit(registry_process, holder, delay_ms) do
    ref = Process.monitor(holder)

    receive do
      {:DOWN, ^ref, :process, ^holder, _reason} ->
        Process.sleep(delay_ms)
        :sys.resume(registry_process)
    end
  end

  @doc """
  Returns whether a process on this node is alive.
  """
  @spec alive?(pid()) :: boolean()
  def alive?(pid), do: Process.alive?(pid)

  @doc """
  Connects this node to `node`.
  """
  @spec connect(node()) :: boolean()
  def connect(node), do: Node.connect(node)

  defp classify_child(pid) do
    case :proc_lib.initial_call(pid) do
      {SingletonProxy, :init, _arity} -> :proxy
      {SingletonHost, :init, _arity} -> :host
      {ClusterTestSingleton, :init, _arity} -> :singleton
      {ClusterTestQuickRejectingSingleton, :init, _arity} -> :singleton
      {ClusterTestRejectingSingleton, :init, _arity} -> :singleton
      _other -> :unknown
    end
  end
end
