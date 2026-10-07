defmodule Commanded.Registration.SynRegistry.ClusterTestApplication do
  @moduledoc """
  An application that a cluster test loads on a peer node at runtime, with a
  supervisor that hosts a `ClusterTestSingleton`.

  A graceful stop of a node stops its applications before it closes its
  connections. A supervisor that belongs to no application is killed only
  after that, so a host under it would look to the other nodes like a host
  on a node that was lost.
  """

  use Application

  alias Commanded.Registration.SynRegistry.ClusterTestNode
  alias Commanded.Registration.SynRegistry.ClusterTestSingleton

  @impl Application
  def start(_type, {adapter_meta, name}) do
    host_spec = ClusterTestNode.build_host_spec(adapter_meta, name, ClusterTestSingleton, [])

    Supervisor.start_link([host_spec], strategy: :one_for_one)
  end
end
