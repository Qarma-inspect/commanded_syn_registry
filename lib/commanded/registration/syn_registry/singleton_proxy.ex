defmodule Commanded.Registration.SynRegistry.SingletonProxy do
  # Local stand-in for a cluster singleton hosted by another process.
  #
  # A registry adapter starts a proxy when the name it registers is already held
  # by another process. The proxy is linked to the host that tried to start the
  # singleton and monitors the process holding the name. What it does when that
  # process goes down depends on the exit reason, the only thing it learns.
  #
  # A reason in `t:failover_reason/0` means the holder's node is lost or
  # stopping, the name pointed at a process that was already gone, or the
  # holder lost a name conflict. The proxy waits a random delay within the
  # configured bounds and then stops with the same reason, and its host starts
  # its child again: the singleton when the name is free, or a new proxy when
  # another node took it first. The delay spreads the retries from all nodes
  # out in time, so the first host to retry registers the name and the rest
  # find it taken.
  #
  # `:noproc` means this node's table still names a holder that has exited:
  # the holder's node has not reported the exit yet, and the parent there may
  # already be starting a replacement. While it waits out its delay, the
  # proxy looks the name up every 10 ms and monitors the first other process
  # it finds there instead of stopping, so the parent on this node sees the
  # replacement's own exits as well. A replacement that exits before the
  # proxy finds it is missed, and one that has exited by the time the proxy
  # monitors it keeps the proxy looking until that delay runs out. When the
  # name still points at an exited process, or at nothing, as the delay runs
  # out, the proxy stops with `:noproc`.
  #
  # Any other reason is the singleton's own exit: a crash, `:normal`, or a stop
  # its `error/3` callback asked for. The proxy stops with that reason at once,
  # its host stops with it too, and the parent on this node applies its restart
  # type, as the parent on the node that ran the singleton does.
  #
  # The proxy is deliberately not registered under the singleton's name and
  # does not forward messages. Callers must reach the singleton through its
  # registered name, which Commanded already does.

  @moduledoc false

  use GenServer

  require Logger

  alias Commanded.Registration.SynRegistry.ConflictResolution

  @typedoc """
  One delay, in milliseconds, drawn from a `t:failover_delay_range/0`.
  """
  @type failover_delay_ms :: non_neg_integer()

  @typedoc """
  Bounds, in milliseconds, of the random delay a proxy waits after the
  singleton it monitors goes down and before it exits itself.
  """
  @type failover_delay_range :: {min_ms :: failover_delay_ms(), max_ms :: failover_delay_ms()}

  @typedoc """
  An exit reason of the process holding the name that the other nodes keep
  from their parents: they take the name over, or proxy a replacement that
  holds it by the end of their failover delay. `:noconnection` comes from a
  lost node, `:shutdown` from a stopping node, and `:killed` from a stopping
  node whose shutdown timeout ran out. `:noproc` means the name pointed at a
  process that was already gone when the proxy started, and
  `{:shutdown, :name_conflict}` that the holder lost a name conflict.
  """
  @type failover_reason :: :noconnection | :shutdown | :killed | :noproc | {:shutdown, :name_conflict}

  # Every failover reason except the conflict loss, whose value
  # `ConflictResolution` owns.
  @failover_exit_reasons [:noconnection, :shutdown, :killed, :noproc]

  # How often a proxy whose holder was gone when it started looks the name up
  # while it waits out its failover delay.
  @holder_lookup_interval_ms 10

  @enforce_keys [:pid, :name, :monitor_ref, :failover_delay_range]
  defstruct [:pid, :name, :monitor_ref, :failover_delay_range, :lookup_deadline_ms]

  @type t :: %__MODULE__{
          pid: pid(),
          name: GenServer.name(),
          monitor_ref: reference(),
          failover_delay_range: failover_delay_range(),
          lookup_deadline_ms: integer() | nil
        }

  defguardp is_valid_failover_delay_range(range)
            when is_tuple(range) and tuple_size(range) == 2 and
                   is_integer(elem(range, 0)) and is_integer(elem(range, 1)) and
                   elem(range, 0) >= 0 and elem(range, 0) <= elem(range, 1)

  @doc """
  Starts a proxy that monitors `pid`, the process currently registered under
  `name`, a name `GenServer.whereis/1` resolves.

  The proxy exits when that process goes down: at once and with the same
  reason when the exit is the process's own, and between `min_ms` and
  `max_ms` later when the reason is a `t:failover_reason/0`. When the process
  was already gone, the proxy monitors the next process registered under
  `name` within that delay instead, if one turns up.
  """
  @spec start_link(holder :: pid(), name :: GenServer.name(), failover_delay_range()) :: GenServer.on_start()
  # `SingletonHost`, the only caller in the library, passes a holder it has
  # matched with `is_pid/1`, the singleton's `{:via, :syn, {scope, name}}`
  # name, and the range that
  # `Commanded.Registration.SynRegistry.child_spec/2` validated when the
  # Commanded application started.
  def start_link(pid, name, failover_delay_range) do
    GenServer.start_link(__MODULE__, {pid, name, failover_delay_range})
  end

  @impl GenServer
  def init({pid, name, failover_delay_range}) do
    monitor_ref = Process.monitor(pid)

    state = %__MODULE__{
      pid: pid,
      name: name,
      monitor_ref: monitor_ref,
      failover_delay_range: failover_delay_range
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, pid, :noproc}, %__MODULE__{monitor_ref: ref, pid: pid} = state) do
    wait_for_new_holder(state)
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, %__MODULE__{monitor_ref: ref, pid: pid} = state) do
    if failover_reason?(reason),
      do: exit_after_failover_delay(state, reason),
      else: pass_exit_on(state, reason)
  end

  def handle_info({:exit_after_failover_delay, reason}, %__MODULE__{} = state), do: exit_with(state, reason)

  def handle_info(:look_up_holder, %__MODULE__{} = state), do: look_up_holder(state)

  def handle_info(_message, %__MODULE__{} = state), do: {:noreply, state}

  @doc """
  Returns whether a proxy treats the holder's exit with `exit_reason` as
  registry churn, kept from its host's parent, rather than as the holder's
  own exit, which it passes on at once.

  It is true for the reasons in `t:failover_reason/0`, which a node's loss, a
  node's shutdown or the registry produces, and false for the holder's own
  exits: a crash, `:normal`, or `{:shutdown, reason}` with any other reason.
  A singleton that stops itself with one of those five reasons is treated
  like one whose node stopped it, so a deliberate stop uses none of them.
  """
  @spec failover_reason?(exit_reason :: term()) :: boolean()
  def failover_reason?(exit_reason) do
    exit_reason in @failover_exit_reasons or ConflictResolution.singleton_conflict_loss?(exit_reason)
  end

  @doc """
  Draws the delay, in milliseconds, that a proxy waits before exiting once the
  singleton it monitors has gone down.

  The delay is uniformly distributed over the inclusive `min_ms..max_ms` range.
  """
  @spec draw_failover_delay(failover_delay_range()) :: failover_delay_ms()
  def draw_failover_delay({min_ms, max_ms}), do: min_ms + :rand.uniform(max_ms - min_ms + 1) - 1

  @doc """
  Returns the failover delay range, or raises `ArgumentError` when it is not a
  `{min_ms, max_ms}` tuple a proxy can draw from.

  Registry adapters call this on the `:failover_delay_range` they read from the
  Commanded application's registry config, so a bad value is rejected at boot
  rather than at the first failover.
  """
  @spec validate_failover_delay_range!(candidate :: term()) :: failover_delay_range()
  def validate_failover_delay_range!(failover_delay_range)
      when is_valid_failover_delay_range(failover_delay_range) do
    failover_delay_range
  end

  def validate_failover_delay_range!(candidate) do
    raise ArgumentError,
          ":failover_delay_range must be a {min_ms, max_ms} tuple of non-negative integers with min <= max, got: " <>
            inspect(candidate)
  end

  defp exit_after_failover_delay(%__MODULE__{} = state, reason) do
    delay_ms = draw_failover_delay(state.failover_delay_range)

    log_singleton_down(state, reason, delay_ms)

    Process.send_after(self(), {:exit_after_failover_delay, reason}, delay_ms)

    {:noreply, state}
  end

  # A replacement that had exited by the time the proxy monitored it reports
  # `:noproc` within the current delay, and the proxy keeps that delay's
  # deadline. A `:noproc` after the deadline starts a new delay.
  defp wait_for_new_holder(%__MODULE__{} = state) do
    if before_lookup_deadline?(state),
      do: look_up_holder(state),
      else: look_up_holder_within_new_delay(state)
  end

  defp look_up_holder_within_new_delay(%__MODULE__{} = state) do
    delay_ms = draw_failover_delay(state.failover_delay_range)
    deadline_ms = System.monotonic_time(:millisecond) + delay_ms

    log_holder_gone(state, delay_ms)

    look_up_holder(%__MODULE__{state | lookup_deadline_ms: deadline_ms})
  end

  # The name resolving to the holder that has gone, or to nothing, keeps the
  # proxy waiting.
  defp look_up_holder(%__MODULE__{pid: gone_holder} = state) do
    case GenServer.whereis(state.name) do
      holder when is_pid(holder) and holder != gone_holder -> monitor_new_holder(state, holder)
      _gone_holder_or_nil -> keep_waiting_for_new_holder(state)
    end
  end

  defp keep_waiting_for_new_holder(%__MODULE__{} = state) do
    if before_lookup_deadline?(state) do
      Process.send_after(self(), :look_up_holder, @holder_lookup_interval_ms)

      {:noreply, state}
    else
      exit_with(state, :noproc)
    end
  end

  # The first `:noproc` finds no deadline and draws a delay.
  defp before_lookup_deadline?(%__MODULE__{lookup_deadline_ms: nil}), do: false

  defp before_lookup_deadline?(%__MODULE__{} = state), do: System.monotonic_time(:millisecond) < state.lookup_deadline_ms

  defp monitor_new_holder(%__MODULE__{} = state, holder) do
    monitor_ref = Process.monitor(holder)
    state = %__MODULE__{state | pid: holder, monitor_ref: monitor_ref}

    log_new_holder(state)

    {:noreply, state}
  end

  # Exits at once. The parent on the holder's node starts the singleton again
  # within milliseconds; a delay here could let the new singleton exit before
  # this node proxies it, and the parent on this node would then count fewer
  # exits than the parent on the holder's node.
  defp pass_exit_on(%__MODULE__{} = state, reason) do
    log_exit_passed_on(state, reason)

    exit_with(state, reason)
  end

  # Terminate with an exit signal rather than `{:stop, reason, state}`.
  # `GenServer` logs a crash report for every stop reason other than
  # `:normal`, `:shutdown` or `{:shutdown, _}`. The holder of a name exits with
  # `:noconnection` when the node hosting it is lost, which is routine, and a
  # crash of the holder is reported on the holder's node already; neither is
  # a fault in this process. The signal produces no report and keeps the
  # holder's exact exit reason, which the host reads to decide whether it
  # starts its child again or exits with that reason itself. The proxy does
  # not trap exits, so the signal terminates it before anything else runs,
  # and a process that sends itself `:normal` exits too.
  #
  # `Process.exit(self(), :kill)` would end the proxy with `:killed`, which
  # the host reads as a failover. A linked process that exits with `:kill`
  # sends a signal that keeps the reason, so the proxy ends with `:kill` as
  # soon as that process has run.
  defp exit_with(%__MODULE__{} = state, :kill) do
    spawn_link(fn -> exit(:kill) end)

    {:noreply, state}
  end

  defp exit_with(%__MODULE__{} = state, reason) do
    Process.exit(self(), reason)

    {:noreply, state}
  end

  defp log_singleton_down(%__MODULE__{} = state, reason, delay_ms) do
    holder_node = node(state.pid)

    [
      "SingletonProxy: singleton down, stopping the proxy:",
      "name=#{inspect(state.name)}",
      "node=#{inspect(holder_node)}",
      "reason=#{inspect(reason)}",
      "delay_ms=#{inspect(delay_ms)}"
    ]
    |> Enum.join(" ")
    |> Logger.info()
  end

  defp log_holder_gone(%__MODULE__{} = state, delay_ms) do
    holder_node = node(state.pid)

    [
      "SingletonProxy: name points at a process that is gone, looking it up again:",
      "name=#{inspect(state.name)}",
      "node=#{inspect(holder_node)}",
      "delay_ms=#{inspect(delay_ms)}"
    ]
    |> Enum.join(" ")
    |> Logger.info()
  end

  defp log_new_holder(%__MODULE__{} = state) do
    holder_node = node(state.pid)

    [
      "SingletonProxy: name taken by a new process, proxying it:",
      "name=#{inspect(state.name)}",
      "node=#{inspect(holder_node)}",
      "pid=#{inspect(state.pid)}"
    ]
    |> Enum.join(" ")
    |> Logger.info()
  end

  defp log_exit_passed_on(%__MODULE__{} = state, reason) do
    holder_node = node(state.pid)

    [
      "SingletonProxy: singleton exited on its own, passing the exit on:",
      "name=#{inspect(state.name)}",
      "node=#{inspect(holder_node)}",
      "reason=#{inspect(reason)}"
    ]
    |> Enum.join(" ")
    |> Logger.info()
  end
end
