defmodule Commanded.Registration.SynRegistry.SingletonProxy do
  # Local stand-in for a cluster singleton hosted by another process.
  #
  # A registry adapter starts a proxy when the name it registers is already held
  # by another process. The proxy is linked to the host that tried to start the
  # singleton and monitors the process holding the name. When that process goes
  # down, the proxy waits a random delay within the configured bounds and then
  # stops with the same exit reason, and the host starts its child again: the
  # singleton when the name is free, or a new proxy when another node took it
  # first. The delay spreads the retries from all nodes out in time, so the
  # first host to retry registers the name and the rest find it taken.
  #
  # The proxy is deliberately not registered under the singleton's name and
  # does not forward messages. Callers must reach the singleton through its
  # registered name, which Commanded already does.

  @moduledoc false

  use GenServer

  require Logger

  @typedoc """
  One delay, in milliseconds, drawn from a `t:failover_delay_range/0`.
  """
  @type failover_delay_ms :: non_neg_integer()

  @typedoc """
  Bounds, in milliseconds, of the random delay a proxy waits after the
  singleton it monitors goes down and before it exits itself.
  """
  @type failover_delay_range :: {min_ms :: failover_delay_ms(), max_ms :: failover_delay_ms()}

  @enforce_keys [:pid, :name, :monitor_ref, :failover_delay_range]
  defstruct [:pid, :name, :monitor_ref, :failover_delay_range]

  @type t :: %__MODULE__{pid: pid(), name: term(), monitor_ref: reference(), failover_delay_range: failover_delay_range()}

  defguardp is_valid_failover_delay_range(range)
            when is_tuple(range) and tuple_size(range) == 2 and
                   is_integer(elem(range, 0)) and is_integer(elem(range, 1)) and
                   elem(range, 0) >= 0 and elem(range, 0) <= elem(range, 1)

  @doc """
  Starts a proxy that monitors `pid`, the process currently registered under
  `name`, and exits between `min_ms` and `max_ms` after that process goes
  down.
  """
  @spec start_link(holder :: pid(), name :: term(), failover_delay_range()) :: GenServer.on_start()
  def start_link(pid, name, failover_delay_range)
      when is_pid(pid) and is_valid_failover_delay_range(failover_delay_range) do
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
  def handle_info({:DOWN, ref, :process, pid, reason}, %__MODULE__{monitor_ref: ref, pid: pid} = state) do
    delay_ms = draw_failover_delay(state.failover_delay_range)

    log_singleton_down(state.name, pid, reason, delay_ms)

    Process.send_after(self(), {:exit_after_failover_delay, reason}, delay_ms)

    {:noreply, state}
  end

  def handle_info({:exit_after_failover_delay, reason}, %__MODULE__{} = state) do
    # Terminate with an exit signal rather than `{:stop, reason, state}`.
    # `GenServer` logs a crash report for every stop reason other than
    # `:normal`, `:shutdown` or `{:shutdown, _}`. The holder of a name exits
    # with `:noconnection` when the node hosting it is lost, which is routine,
    # not a fault in this process, and would be reported. The signal produces
    # no report and keeps the holder's exact exit reason, which the host logs
    # when it starts its child again. The proxy does not trap exits, so the
    # signal terminates it before anything else runs.
    Process.exit(self(), reason)

    {:noreply, state}
  end

  def handle_info(_message, %__MODULE__{} = state), do: {:noreply, state}

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

  defp log_singleton_down(name, pid, reason, delay_ms) do
    holder_node = node(pid)

    [
      "SingletonProxy: singleton down, stopping the proxy:",
      "name=#{inspect(name)}",
      "node=#{inspect(holder_node)}",
      "reason=#{inspect(reason)}",
      "delay_ms=#{inspect(delay_ms)}"
    ]
    |> Enum.join(" ")
    |> Logger.info()
  end
end
