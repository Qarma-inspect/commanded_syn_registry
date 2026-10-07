defmodule Commanded.Registration.SynRegistry.SingletonReaper do
  # Kills the singleton of a host that was killed.
  #
  # A Commanded event handler traps exits. When its host dies, the exit
  # reaches the handler as a message, which it reads only once its current
  # callback returns, and until then it keeps its name and its subscription.
  # The parent kills the host when the host outlives the parent's shutdown
  # value, or at once under `:brutal_kill`, and the reaper then kills the
  # singleton, so the parent's shutdown value is also the limit on how long
  # the singleton keeps its name.
  #
  # The host starts its reaper before its first child. The reaper is not
  # linked to the host; it monitors it. Before each start the host tells the
  # reaper that a start is under way, and after it which singleton to guard,
  # if the start produced one. On the host's `DOWN` the reaper kills the last
  # singleton the host told it about. It also kills the local process
  # registered under the name if the host started it. That covers a singleton
  # still inside its start: `gen` registers the name before it calls `init/1`,
  # so the host learns the pid only after the singleton's `init/1` returns,
  # and a singleton that traps exits in `init/1` outlives a host killed in
  # that window.
  #
  # The singleton can also lose its name in that window. Conflict resolution
  # drops the name of a starting singleton without stopping it: the request
  # to stop waits until the singleton's current callback returns, and a
  # handler can block in `handle_continue/2` while it subscribes. When the
  # host dies in the middle of a start and the name leads to no process the
  # host started, the reaper therefore looks through `Process.list/0` once
  # for a process whose parent is the host. The cost of that scan grows with
  # the number of processes on the node, so it runs in this case only.
  #
  # The reaper runs code of this module only, so reloading the host's module
  # leaves it alone. Purging an old version of this module kills the reapers
  # still running it; the host monitors its reaper and starts a new one. It
  # also checks that its reaper is alive before each start, so no singleton
  # starts unguarded while the old reaper's `DOWN` still waits in the host's
  # mailbox. A reaper that dies while the host is inside a start is replaced
  # once the start returns; a host killed inside that start leaves the
  # singleton it was starting unguarded.

  @moduledoc false

  @enforce_keys [:host, :host_ref, :scope, :name]
  defstruct [:host, :host_ref, :scope, :name, :singleton, starting?: false]

  @type t :: %__MODULE__{
          host: pid(),
          host_ref: reference(),
          scope: atom(),
          name: term(),
          singleton: pid() | nil,
          starting?: boolean()
        }

  @doc """
  Starts a reaper for the calling host, which hosts the singleton registered
  under `name` in `scope`, and returns it with a monitor on it.

  The reaper monitors the host before this returns, so the host may start its
  singleton right after.
  """
  @spec start_monitor(scope :: atom(), name :: term()) :: {reaper :: pid(), reaper_ref :: reference()}
  def start_monitor(scope, name) do
    {{:ok, reaper}, reaper_ref} = :proc_lib.start_monitor(__MODULE__, :init, [self(), scope, name])

    {reaper, reaper_ref}
  end

  @doc """
  Tells `reaper` that the host is starting a child, whose pid the host learns
  only when the start returns. The start lasts until the next `guard/2`.
  """
  @spec announce_start(reaper :: pid()) :: :ok
  def announce_start(reaper) do
    send(reaper, :starting)

    :ok
  end

  @doc """
  Tells `reaper` to kill `singleton` when the host dies, in place of the
  singleton it guarded so far, and ends the start announced with
  `announce_start/1`. With `nil` the reaper guards no singleton, which is the
  case after a start that produced a proxy or failed.
  """
  @spec guard(reaper :: pid(), singleton :: pid() | nil) :: :ok
  def guard(reaper, singleton) do
    send(reaper, {:guard, singleton})

    :ok
  end

  @doc false
  @spec init(host :: pid(), scope :: atom(), name :: term()) :: :ok
  def init(host, scope, name) do
    host_ref = Process.monitor(host)
    :ok = :proc_lib.init_ack(host, {:ok, self()})

    await_host_down(%__MODULE__{host: host, host_ref: host_ref, scope: scope, name: name})
  end

  # The host sends `:starting` and `{:guard, pid}` before it can die, and
  # these messages and the `DOWN` all come from the host, so the reaper reads
  # the state of the last start before the host's `DOWN`.
  defp await_host_down(%__MODULE__{host_ref: host_ref} = state) do
    receive do
      :starting -> await_host_down(%__MODULE__{state | starting?: true})
      {:guard, singleton} -> await_host_down(%__MODULE__{state | singleton: singleton, starting?: false})
      {:DOWN, ^host_ref, :process, _host, _reason} -> kill_singletons_of_host(state)
    end
  end

  # Every process is killed by pid, and only when the host started it, so a
  # singleton of a newer host under the same name is left alone. The host
  # starts a child only after the previous one has exited, so during a start
  # the guarded singleton is already gone and only the name or the scan can
  # lead to the new one.
  defp kill_singletons_of_host(%__MODULE__{} = state) do
    if state.singleton, do: Process.exit(state.singleton, :kill)

    holder_killed? = kill_local_holder(state)
    if state.starting? and not holder_killed?, do: kill_processes_started_by(state.host)

    :ok
  end

  defp kill_local_holder(%__MODULE__{} = state) do
    case lookup_holder(state) do
      {holder, _metadata} when node(holder) == node() -> kill_if_started_by(holder, state.host)
      _remote_holder_or_none -> false
    end
  end

  # syn's lookup raises once syn's tables are gone, after a crash of
  # `syn_backbone` or a stop of the `:syn` application. The reaper then finds
  # no holder, and a start under way still gets the scan.
  defp lookup_holder(%__MODULE__{} = state) do
    :syn.lookup(state.scope, state.name)
  catch
    :error, _tables_gone -> :undefined
  end

  # The host started the reaper too, so the reaper leaves itself out.
  defp kill_processes_started_by(host) do
    Process.list()
    |> List.delete(self())
    |> Enum.each(&kill_if_started_by(&1, host))
  end

  defp kill_if_started_by(pid, host) do
    started_by_host? = Process.info(pid, :parent) == {:parent, host}
    if started_by_host?, do: Process.exit(pid, :kill)

    started_by_host?
  end
end
