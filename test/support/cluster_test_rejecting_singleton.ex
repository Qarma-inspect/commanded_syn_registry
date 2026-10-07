defmodule Commanded.Registration.SynRegistry.ClusterTestRejectingSingleton do
  @moduledoc """
  A singleton for the registry's cluster tests that stops with `:rejected`
  300 ms after each start, the way an event handler does whose `error/3`
  callback stops it on the same event every time. On a node where
  `hold_first_stop/0` was called, a singleton that starts before the first
  stop there waits for `reject/1` instead, so a test can set up the other
  node first. It counts these stops on its node for `count_stops/0`.
  """

  use GenServer

  @stop_after_ms 300

  @doc """
  Returns how many times a singleton of this module has stopped with
  `:rejected` on this node.
  """
  @spec count_stops() :: non_neg_integer()
  def count_stops, do: :persistent_term.get({__MODULE__, :stops}, 0)

  @doc """
  Holds back the first stop on this node: a singleton of this module that
  starts here before any has stopped waits for `reject/1` instead of
  stopping on its own.
  """
  @spec hold_first_stop() :: :ok
  def hold_first_stop, do: :persistent_term.put({__MODULE__, :first_stop_held?}, true)

  @doc """
  Makes `singleton` stop with `:rejected`.
  """
  @spec reject(singleton :: pid()) :: :ok
  def reject(singleton) do
    send(singleton, :reject)

    :ok
  end

  @impl GenServer
  def init(state) do
    if count_stops() > 0 or not first_stop_held?(), do: Process.send_after(self(), :reject, @stop_after_ms)

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:reject, state) do
    :persistent_term.put({__MODULE__, :stops}, count_stops() + 1)

    {:stop, :rejected, state}
  end

  defp first_stop_held?, do: :persistent_term.get({__MODULE__, :first_stop_held?}, false)
end
