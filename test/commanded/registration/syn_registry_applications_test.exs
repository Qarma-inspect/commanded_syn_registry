defmodule Commanded.Registration.SynRegistryApplicationsTest do
  use ExUnit.Case, async: false

  import Commanded.Registration.SynRegistry.SupervisorChildren

  alias Commanded.Event.Handler
  alias Commanded.Registration
  alias Commanded.Registration.SynRegistry
  alias Commanded.Registration.SynRegistry.SingletonProxy

  @handler_name "shared_handler_name"

  defmodule FirstApp do
    use Commanded.Application,
      otp_app: :commanded_syn_registry,
      event_store: [adapter: Commanded.EventStore.Adapters.InMemory],
      registry: [adapter: SynRegistry, failover_delay_range: {0, 0}]
  end

  defmodule SecondApp do
    use Commanded.Application,
      otp_app: :commanded_syn_registry,
      event_store: [adapter: Commanded.EventStore.Adapters.InMemory],
      registry: [adapter: SynRegistry, failover_delay_range: {0, 0}]
  end

  defmodule FirstHandler do
    use Commanded.Event.Handler, application: FirstApp, name: "shared_handler_name"
  end

  defmodule SecondHandler do
    use Commanded.Event.Handler, application: SecondApp, name: "shared_handler_name"
  end

  defmodule Tree do
    use Supervisor

    def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

    @impl Supervisor
    def init(_arg) do
      children = [FirstApp, SecondApp, FirstHandler, SecondHandler]

      Supervisor.init(children, strategy: :one_for_one)
    end
  end

  setup do
    start_supervised!(Tree)
    first = Registration.whereis_name(FirstApp, Handler.name(FirstApp, @handler_name))
    second = Registration.whereis_name(SecondApp, Handler.name(SecondApp, @handler_name))
    [first: first, second: second]
  end

  describe "two applications on one node" do
    test "run an event handler of the same name each in its own scope", ctx do
      %{first: first, second: second} = ctx

      assert is_pid(first)
      assert is_pid(second)
      assert first != second
      assert Process.alive?(first)
      assert Process.alive?(second)
    end

    test "host each handler itself rather than a proxy for the other one", ctx do
      %{first: first, second: second} = ctx
      first_child = fetch_singleton_child(FirstHandler)
      second_child = fetch_singleton_child(SecondHandler)

      assert first_child == first
      assert second_child == second
      refute match?(%SingletonProxy{}, :sys.get_state(first_child))
      refute match?(%SingletonProxy{}, :sys.get_state(second_child))
    end
  end

  defp fetch_singleton_child(handler_module) do
    host = fetch_child(Tree, handler_module)

    fetch_child(host)
  end
end
