defmodule Commanded.Registration.SynRegistryConsistencyTest do
  use ExUnit.Case, async: false

  alias Commanded.Registration.SynRegistry

  @collector :syn_registry_consistency_test_collector
  @consistency_timeout_ms 300

  defmodule Request do
    @enforce_keys [:order_id]
    defstruct [:order_id]

    @type t :: %__MODULE__{order_id: String.t()}
  end

  defmodule Follow do
    @enforce_keys [:order_id]
    defstruct [:order_id]

    @type t :: %__MODULE__{order_id: String.t()}
  end

  defmodule Requested do
    @enforce_keys [:order_id]
    defstruct [:order_id]

    @type t :: %__MODULE__{order_id: String.t()}
  end

  defmodule Followed do
    @enforce_keys [:order_id]
    defstruct [:order_id]

    @type t :: %__MODULE__{order_id: String.t()}
  end

  defmodule Order do
    defstruct []

    @type t :: %__MODULE__{}

    def execute(%Order{}, %Request{order_id: order_id}), do: %Requested{order_id: order_id}
    def execute(%Order{}, %Follow{order_id: order_id}), do: %Followed{order_id: order_id}

    def apply(%Order{} = order, _event), do: order
  end

  defmodule OrderRouter do
    use Commanded.Commands.Router

    identify Order, by: :order_id
    dispatch [Request, Follow], to: Order
  end

  defmodule App do
    use Commanded.Application,
      otp_app: :commanded_syn_registry,
      event_store: [adapter: Commanded.EventStore.Adapters.InMemory],
      registry: [adapter: SynRegistry, failover_delay_range: {0, 0}]

    router OrderRouter
  end

  defmodule StrongHandler do
    use Commanded.Event.Handler,
      application: App,
      name: "syn_registry_consistency_test_handler",
      consistency: :strong

    def handle(%Requested{order_id: order_id}, _metadata) do
      result = App.dispatch(%Follow{order_id: order_id}, consistency: :strong)
      send(:syn_registry_consistency_test_collector, {:follow_dispatched, result})

      :ok
    end

    def handle(%Followed{}, _metadata), do: :ok
  end

  defmodule Tree do
    use Supervisor

    def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

    @impl Supervisor
    def init(_arg), do: Supervisor.init([App, StrongHandler], strategy: :one_for_one)
  end

  setup do
    previous_timeout = Application.get_env(:commanded, :dispatch_consistency_timeout)
    Application.put_env(:commanded, :dispatch_consistency_timeout, @consistency_timeout_ms)
    on_exit(fn -> restore_consistency_timeout(previous_timeout) end)

    Process.register(self(), @collector)
    start_supervised!(Tree)

    :ok
  end

  describe "a strongly consistent event handler dispatching from handle/2" do
    # The registry hands Commanded the pid of the handler's host process,
    # and Commanded 1.4.11 excludes only the dispatching process from the
    # strong consistency wait. The handler therefore waits for its own
    # acknowledgement of the event its command produced. Fixed in Commanded by
    # https://github.com/commanded/commanded/pull/668. When a Commanded
    # release contains that fix, this test fails and its expectation flips to
    # `:ok`.
    test "gets :consistency_timeout for a strong command to its own application" do
      assert App.dispatch(%Request{order_id: "order-1"}, consistency: :eventual) == :ok

      assert_receive {:follow_dispatched, result}, 5 * @consistency_timeout_ms
      assert result == {:error, :consistency_timeout}
    end
  end

  defp restore_consistency_timeout(nil),
    do: Application.delete_env(:commanded, :dispatch_consistency_timeout)

  defp restore_consistency_timeout(timeout),
    do: Application.put_env(:commanded, :dispatch_consistency_timeout, timeout)
end
