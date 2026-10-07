defmodule Commanded.Registration.SynRegistry.RefusingProcess do
  @moduledoc """
  A process whose start fails the way a refused syn registration does when the
  holder of the name is gone by the time it is looked up:
  `{:error, {:already_started, :undefined}}`.

  The first `refusals` starts fail and the next one succeeds, and
  `refuse_next_starts/2` makes later starts fail the same way. The count and
  the number of starts live in an `Agent`, so a test can read how many times
  the adapter tried.
  """

  use GenServer, restart: :temporary

  @typedoc """
  The `Agent` that counts starts and decides which of them are refused.
  """
  @type counter :: pid()

  @doc """
  Starts the counter that refuses the first `refusals` starts.
  """
  @spec start_counter(refusals :: non_neg_integer()) :: counter()
  def start_counter(refusals) do
    {:ok, counter} = Agent.start_link(fn -> %{refusals_left: refusals, starts: 0} end)

    counter
  end

  @doc """
  Makes the next `refusals` starts against `counter` fail.
  """
  @spec refuse_next_starts(counter(), non_neg_integer()) :: :ok
  def refuse_next_starts(counter, refusals), do: Agent.update(counter, &%{&1 | refusals_left: refusals})

  @doc """
  Returns how many times a process was started against `counter`.
  """
  @spec fetch_start_count(counter()) :: non_neg_integer()
  def fetch_start_count(counter), do: Agent.get(counter, & &1.starts)

  @doc """
  Starts the process under the registered name passed in `opts`, the entry
  point of an aggregate.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :counter), name: name)
  end

  @impl GenServer
  def init(counter) do
    refuse? = Agent.get_and_update(counter, &count_start/1)

    if refuse?,
      do: {:stop, {:already_started, :undefined}},
      else: {:ok, counter}
  end

  defp count_start(%{refusals_left: left, starts: starts} = state) when left > 0,
    do: {true, %{state | refusals_left: left - 1, starts: starts + 1}}

  defp count_start(%{starts: starts} = state), do: {false, %{state | starts: starts + 1}}
end
