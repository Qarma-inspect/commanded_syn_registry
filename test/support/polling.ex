defmodule Commanded.Registration.SynRegistry.Polling do
  @moduledoc """
  Waits for a state that the code under test reaches in a process of its own,
  by calling a probe every 10 ms until the probe finds the state or the time
  runs out.
  """

  import ExUnit.Assertions

  @poll_interval_ms 10

  @typedoc """
  How long `await/2` keeps calling the probe before it fails the test, in
  milliseconds.
  """
  @type timeout_ms :: pos_integer()

  @typedoc """
  What the probe hands back once it finds the awaited state.
  """
  @type awaited_value :: term()

  @typedoc """
  The message the test fails with when the probe still has not found the
  awaited state at the deadline.
  """
  @type failure_message :: String.t()

  @typedoc """
  A function that checks once for the awaited state. It returns
  `{:ok, awaited_value}` when it finds the state, and
  `{:error, failure_message}` describing what it found instead when it does
  not.
  """
  @type probe :: (-> {:ok, awaited_value()} | {:error, failure_message()})

  @doc """
  Calls `probe` every 10 ms until it returns `{:ok, awaited_value}`, and
  returns that value. Fails the test with the message of the probe's last
  `{:error, failure_message}` once `timeout_ms` has passed.
  """
  @spec await(timeout_ms(), probe()) :: awaited_value()
  def await(timeout_ms, probe) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    poll_until(deadline, probe)
  end

  defp poll_until(deadline, probe) do
    case probe.() do
      {:ok, awaited_value} ->
        awaited_value

      {:error, failure_message} ->
        if System.monotonic_time(:millisecond) > deadline, do: flunk(failure_message)

        Process.sleep(@poll_interval_ms)
        poll_until(deadline, probe)
    end
  end
end
