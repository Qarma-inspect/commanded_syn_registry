defmodule Commanded.Registration.SynRegistry.ClusterTestSingleton do
  @moduledoc """
  The process registered under a syn name in the registry's cluster tests.

  It plays both roles the adapter supports: a singleton started through
  `Commanded.Registration.SynRegistry.start_link/5`, which builds its own
  child spec, and an aggregate started through `start_child/4`, which takes
  the child spec below. `restart: :temporary` is the one
  `Commanded.Aggregates.Aggregate` carries.
  """

  use GenServer, restart: :temporary

  @doc """
  Starts the process under the registered name passed in `opts`.

  This is the aggregate entry point: `start_child/4` puts the syn name into
  the child spec's arguments.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl GenServer
  def init(state), do: {:ok, state}

  @impl GenServer
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}
end
