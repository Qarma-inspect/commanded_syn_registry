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
  alias Commanded.Registration.SynRegistry.ClusterTestSingleton
  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.RecordingEventHandler
  alias Commanded.Registration.SynRegistry.SingletonHost
  alias Commanded.Registration.SynRegistry.SingletonProxy
  alias Commanded.Registration.SynRegistry.SingletonReaper

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
  Tells the syn event handler on this node that `pid`, which may run on
  another node, lost `name` in a conflict in `scope`, as syn does on every
  node that learns of the loss.
  """
  @spec report_conflict_loss(scope :: atom(), name :: term(), loser :: pid(), metadata :: term()) ::
          term()
  def report_conflict_loss(scope, name, pid, metadata) do
    ConflictResolution.on_process_unregistered(
      scope,
      name,
      pid,
      metadata,
      :syn_conflict_resolution
    )
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
  @spec start_registry(module(), SingletonProxy.failover_delay_range()) ::
          SingletonHost.adapter_meta()
  def start_registry(application, failover_delay_range) do
    {:ok, _started} = Application.ensure_all_started(:syn)

    {:ok, [], adapter_meta} =
      SynRegistry.child_spec(application, failover_delay_range: failover_delay_range)

    adapter_meta
  end

  @doc """
  Starts a singleton for `name` and returns its host process.
  """
  @spec start_singleton(SingletonHost.adapter_meta(), term()) :: pid()
  def start_singleton(adapter_meta, name) do
    {:ok, host} = SynRegistry.start_link(adapter_meta, name, ClusterTestSingleton, :state, [])

    Process.unlink(host)

    host
  end

  @doc """
  Starts a process for `name` the way an aggregate is started, under a
  `DynamicSupervisor` of this node, and returns the process.
  """
  @spec start_aggregate(SingletonHost.adapter_meta(), term()) :: pid()
  def start_aggregate(adapter_meta, name) do
    {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    Process.unlink(supervisor)

    {:ok, pid} =
      SynRegistry.start_child(adapter_meta, name, supervisor, {ClusterTestSingleton, []})

    pid
  end

  @doc """
  Returns the child of a singleton's host process and what it is: the
  registered process itself or a proxy for the node holding the name.
  """
  @spec child(pid()) :: {pid(), :singleton | :proxy | :unknown} | :restarting
  def child(host) do
    case Supervisor.which_children(host) do
      [{_id, pid, _type, _modules}] when is_pid(pid) -> {pid, kind_of(pid)}
      _children -> :restarting
    end
  end

  @doc """
  Returns the process this node resolves `name` to.
  """
  @spec whereis(SingletonHost.adapter_meta(), term()) :: pid() | :undefined
  def whereis(adapter_meta, name), do: SynRegistry.whereis_name(adapter_meta, name)

  @doc """
  Starts watching `pid` so that `exit_reason/1` can report how it ended.
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
  @spec exit_reason(pid()) :: term()
  def exit_reason(pid), do: :persistent_term.get({__MODULE__, pid}, nil)

  @doc """
  Starts a reaper for a stand-in host that runs no singleton, kills the host,
  and returns the pid of the reaper. `exit_reason/1` tells how the reaper
  ended once `watch_exit/1` was called on it.
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
  @spec run_stand_in_host(pid(), atom(), term()) :: no_return()
  def run_stand_in_host(caller, scope, name) do
    {reaper, _reaper_ref} = SingletonReaper.start_monitor(scope, name)
    send(caller, {:reaper, reaper})

    Process.sleep(:infinity)
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

  defp kind_of(pid) do
    case :proc_lib.initial_call(pid) do
      {SingletonProxy, :init, _arity} -> :proxy
      {ClusterTestSingleton, :init, _arity} -> :singleton
      _other -> :unknown
    end
  end
end
