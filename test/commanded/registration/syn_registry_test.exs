defmodule Commanded.Registration.SynRegistryTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Commanded.Aggregates.Aggregate, as: CommandedAggregate
  alias Commanded.Event.Handler
  alias Commanded.ProcessManagers.ProcessRouter
  alias Commanded.Registration
  alias Commanded.Registration.SynRegistry
  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.RefusingProcess
  alias Commanded.Registration.SynRegistry.SingletonProxy

  defmodule Singleton do
    use GenServer

    def init(arg), do: {:ok, arg}

    def handle_call(:ping, _from, state), do: {:reply, :pong, state}
  end

  defmodule Aggregate do
    use GenServer

    def start_link(opts) do
      {name, opts} = Keyword.pop(opts, :name)

      GenServer.start_link(__MODULE__, opts, name: name)
    end

    def init(opts), do: {:ok, opts}
  end

  defmodule Increment do
    @enforce_keys [:counter_id]
    defstruct [:counter_id]

    @type t :: %__MODULE__{counter_id: String.t()}
  end

  defmodule IncrementSlowly do
    @enforce_keys [:counter_id]
    defstruct [:counter_id]

    @type t :: %__MODULE__{counter_id: String.t()}
  end

  defmodule Incremented do
    @enforce_keys [:counter_id]
    defstruct [:counter_id]

    @type t :: %__MODULE__{counter_id: String.t()}
  end

  defmodule Counter do
    defstruct count: 0

    @type t :: %__MODULE__{count: non_neg_integer()}

    # Long enough for the test to stop the aggregate and dispatch a second
    # command while this one still occupies the process.
    @slow_execution_ms 200

    def execute(%Counter{}, %IncrementSlowly{counter_id: counter_id}) do
      Process.sleep(@slow_execution_ms)

      %Incremented{counter_id: counter_id}
    end

    def execute(%Counter{}, %Increment{counter_id: counter_id}),
      do: %Incremented{counter_id: counter_id}

    def apply(%Counter{count: count} = counter, %Incremented{}),
      do: %Counter{counter | count: count + 1}
  end

  defmodule CounterRouter do
    use Commanded.Commands.Router

    identify Counter, by: :counter_id
    dispatch [Increment, IncrementSlowly], to: Counter
  end

  @handler_name "syn_registry_test_handler"
  @process_manager_name "syn_registry_test_process_manager"

  defmodule CommandedApp do
    use Commanded.Application,
      otp_app: :commanded_syn_registry,
      event_store: [adapter: Commanded.EventStore.Adapters.InMemory],
      registry: [adapter: SynRegistry, failover_delay_range: {0, 0}]

    router CounterRouter
  end

  defmodule EventHandler do
    use Commanded.Event.Handler, application: CommandedApp, name: "syn_registry_test_handler"
  end

  # Gives up on every counter it sees: rejects the event and asks Commanded to
  # stop the handler.
  defmodule StoppingEventHandler do
    use Commanded.Event.Handler,
      application: CommandedApp,
      name: "syn_registry_test_stopping_handler"

    def handle(%Incremented{}, _metadata), do: {:error, :rejected}

    def error({:error, :rejected}, %Incremented{counter_id: counter_id}, _failure_context),
      do: {:stop, {:rejected, counter_id}}
  end

  defmodule ProcessManager do
    use Commanded.ProcessManagers.ProcessManager,
      application: CommandedApp,
      name: "syn_registry_test_process_manager"

    defstruct []
  end

  # The shape of a production supervision tree: the Commanded application with
  # an event handler and a process manager as its siblings.
  defmodule CommandedTree do
    use Supervisor

    def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

    @impl Supervisor
    def init(_arg),
      do: Supervisor.init([CommandedApp, EventHandler, ProcessManager], strategy: :one_for_one)
  end

  @application __MODULE__.App

  setup do
    {:ok, [], adapter_meta} = SynRegistry.child_spec(@application, failover_delay_range: {0, 0})
    # The adapter resolves names through the scope, never the application.
    adapter_meta = %{adapter_meta | application: __MODULE__.Other}
    name = {:handler, make_ref()}
    [adapter_meta: adapter_meta, name: name]
  end

  describe "child_spec/2" do
    test "starts no process of its own and scopes the registry by application" do
      assert {:ok, [], %{application: @application, scope: @application}} =
               SynRegistry.child_spec(@application, [])
    end

    test "defaults the failover delay range to 200..1000 ms" do
      assert {:ok, [], %{failover_delay_range: {200, 1_000}}} =
               SynRegistry.child_spec(@application, [])
    end

    test "takes the failover delay range from the registry config" do
      assert {:ok, [], %{failover_delay_range: {50, 75}}} =
               SynRegistry.child_spec(@application, failover_delay_range: {50, 75})
    end

    test "rejects a failover delay range whose minimum exceeds its maximum" do
      assert_raise ArgumentError, fn ->
        SynRegistry.child_spec(@application, failover_delay_range: {500, 100})
      end
    end

    test "rejects an unknown option, naming the application and the option" do
      config = [failover_delay: {50, 75}]

      error = assert_raise ArgumentError, fn -> SynRegistry.child_spec(@application, config) end

      assert error.message =~ "for Commanded application #{inspect(@application)}:"
      assert error.message =~ "unknown keys [:failover_delay]"
    end

    test "rejects an option given twice, naming the application and the option" do
      config = [failover_delay_range: {50, 75}, failover_delay_range: {0, 0}]

      error = assert_raise ArgumentError, fn -> SynRegistry.child_spec(@application, config) end

      assert error.message =~ "for Commanded application #{inspect(@application)}:"
      assert error.message =~ "duplicate keys [:failover_delay_range]"
    end

    test "adds the application's scope to the node" do
      {:ok, [], _adapter_meta} = SynRegistry.child_spec(@application, [])

      assert @application in :syn.node_scopes()
    end

    test "installs the conflict resolution event handler" do
      {:ok, [], _adapter_meta} = SynRegistry.child_spec(@application, [])

      assert Application.get_env(:syn, :event_handler) == ConflictResolution
    end
  end

  describe "supervisor_child_spec/3" do
    test "returns a supervisor child spec starting the module with the given argument", ctx do
      %{adapter_meta: adapter_meta} = ctx

      assert SynRegistry.supervisor_child_spec(adapter_meta, CommandedTree, :arg) ==
               %{
                 id: CommandedTree,
                 start: {CommandedTree, :start_link, [:arg]},
                 type: :supervisor
               }
    end
  end

  describe "via_tuple/2 and whereis_name/2" do
    test "returns a via tuple for syn naming the application's scope and the name", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      assert SynRegistry.via_tuple(adapter_meta, name) == {:via, :syn, {@application, name}}
    end

    test "report a name that nobody holds as undefined", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      assert SynRegistry.whereis_name(adapter_meta, name) == :undefined
    end
  end

  describe "start_link/5" do
    test "returns a host process that runs the registered process", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      assert {:ok, host} = SynRegistry.start_link(adapter_meta, name, Singleton, :state, [])

      singleton = child_pid(host)
      assert SynRegistry.whereis_name(adapter_meta, name) == singleton
      assert GenServer.call(singleton, :ping) == :pong
    end
  end

  describe "start_link/5 in two application scopes" do
    test "hosts a process under the same name in each scope", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      {:ok, [], other_meta} = SynRegistry.child_spec(__MODULE__.OtherApp, [])

      {:ok, host} = SynRegistry.start_link(adapter_meta, name, Singleton, :first, [])
      {:ok, other_host} = SynRegistry.start_link(other_meta, name, Singleton, :second, [])

      singleton = child_pid(host)
      other_singleton = child_pid(other_host)
      assert singleton != other_singleton
      assert SynRegistry.whereis_name(adapter_meta, name) == singleton
      assert SynRegistry.whereis_name(other_meta, name) == other_singleton
      assert :sys.get_state(singleton) == :first
      assert :sys.get_state(other_singleton) == :second
    end
  end

  describe "start_link/5 options" do
    test "passes the start options on to the process", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      {:ok, host} =
        SynRegistry.start_link(adapter_meta, name, Singleton, :state,
          spawn_opt: [priority: :high]
        )

      assert Process.info(child_pid(host), :priority) == {:priority, :high}
    end
  end

  describe "start_child/4" do
    setup do
      supervisor = start_supervised!({DynamicSupervisor, strategy: :one_for_one})
      [supervisor: supervisor]
    end

    test "registers the process it starts under the name", ctx do
      %{adapter_meta: adapter_meta, name: name, supervisor: supervisor} = ctx

      assert {:ok, pid} = SynRegistry.start_child(adapter_meta, name, supervisor, {Aggregate, []})

      assert SynRegistry.whereis_name(adapter_meta, name) == pid
    end

    test "returns the process that already holds the name", ctx do
      %{adapter_meta: adapter_meta, name: name, supervisor: supervisor} = ctx
      {:ok, pid} = SynRegistry.start_child(adapter_meta, name, supervisor, {Aggregate, []})

      assert SynRegistry.start_child(adapter_meta, name, supervisor, {Aggregate, []}) ==
               {:ok, pid}
    end

    test "registers a process given as a bare module under the name", ctx do
      %{adapter_meta: adapter_meta, name: name, supervisor: supervisor} = ctx

      assert {:ok, pid} = SynRegistry.start_child(adapter_meta, name, supervisor, Aggregate)

      assert SynRegistry.whereis_name(adapter_meta, name) == pid
    end

    test "registers the process under the syn name even when its arguments carry another name",
         ctx do
      %{adapter_meta: adapter_meta, name: name, supervisor: supervisor} = ctx

      assert {:ok, pid} =
               SynRegistry.start_child(
                 adapter_meta,
                 name,
                 supervisor,
                 {Aggregate, name: :ignored_name}
               )

      assert SynRegistry.whereis_name(adapter_meta, name) == pid
      assert Process.whereis(:ignored_name) == nil
    end

    test "stamps the registration with the start time in nanoseconds", ctx do
      %{adapter_meta: adapter_meta, name: name, supervisor: supervisor} = ctx
      before_start = System.system_time(:nanosecond)

      {:ok, pid} = SynRegistry.start_child(adapter_meta, name, supervisor, {Aggregate, []})

      after_start = System.system_time(:nanosecond)
      assert {^pid, %{started_at: started_at}} = :syn.lookup(@application, name)
      assert started_at in before_start..after_start
    end

    test "registers the process as an aggregate", ctx do
      %{adapter_meta: adapter_meta, name: name, supervisor: supervisor} = ctx

      {:ok, pid} = SynRegistry.start_child(adapter_meta, name, supervisor, {Aggregate, []})

      assert {^pid, %{kind: :aggregate}} = :syn.lookup(@application, name)
    end

    test "starts the process once the refused registration has cleared", ctx do
      %{adapter_meta: adapter_meta, name: name, supervisor: supervisor} = ctx
      counter = RefusingProcess.start_counter(2)

      assert {:ok, pid} =
               SynRegistry.start_child(
                 adapter_meta,
                 name,
                 supervisor,
                 {RefusingProcess, [counter: counter]}
               )

      assert SynRegistry.whereis_name(adapter_meta, name) == pid
      assert RefusingProcess.starts(counter) == 3
    end

    test "gives up with the start error after five attempts when the name never clears", ctx do
      %{adapter_meta: adapter_meta, name: name, supervisor: supervisor} = ctx
      counter = RefusingProcess.start_counter(100)

      assert SynRegistry.start_child(
               adapter_meta,
               name,
               supervisor,
               {RefusingProcess, [counter: counter]}
             ) ==
               {:error, {:already_started, :undefined}}

      assert RefusingProcess.starts(counter) == 5
    end
  end

  describe "handle_call/3 and handle_cast/2" do
    test "raise on a call the process has no clause for" do
      message =
        "attempted to call GenServer #{inspect(self())} but no handle_call/3 clause was provided"

      assert_raise RuntimeError, message, fn -> SynRegistry.handle_call(:request, self(), %{}) end
    end

    test "raise on a cast the process has no clause for" do
      message =
        "attempted to cast GenServer #{inspect(self())} but no handle_cast/2 clause was provided"

      assert_raise RuntimeError, message, fn -> SynRegistry.handle_cast(:request, %{}) end
    end

    test "name a registered process by its registered name" do
      Process.register(self(), :syn_registry_test_named_caller)

      call_message =
        "attempted to call GenServer :syn_registry_test_named_caller but no handle_call/3"

      cast_message =
        "attempted to cast GenServer :syn_registry_test_named_caller but no handle_cast/2"

      assert_raise RuntimeError, ~r/^#{call_message}/, fn ->
        SynRegistry.handle_call(:request, self(), %{})
      end

      assert_raise RuntimeError, ~r/^#{cast_message}/, fn ->
        SynRegistry.handle_cast(:request, %{})
      end
    end
  end

  describe "handle_info/2" do
    setup do
      previous_level = Logger.level()
      Logger.configure(level: :debug)
      on_exit(fn -> Logger.configure(level: previous_level) end)
    end

    test "logs the process and the message it has no clause for and leaves the state unchanged" do
      message = {:unexpected, make_ref()}
      state = %{count: 1}

      log =
        capture_log(fn -> assert SynRegistry.handle_info(message, state) == {:noreply, state} end)

      assert log =~ "[debug]"

      assert log =~
               "SynRegistry: unexpected message: process=#{inspect(self())} message=#{inspect(message)}"
    end

    test "names a registered process by its registered name in the log" do
      Process.register(self(), :syn_registry_test_named_process)

      log = capture_log(fn -> SynRegistry.handle_info(:unexpected, %{}) end)

      assert log =~ "process=:syn_registry_test_named_process message=:unexpected"
    end
  end

  describe "in a Commanded application" do
    setup do
      start_supervised!(CommandedTree)
      handler = Registration.whereis_name(CommandedApp, Handler.name(CommandedApp, @handler_name))

      router =
        Registration.whereis_name(
          CommandedApp,
          ProcessRouter.name(CommandedApp, @process_manager_name)
        )

      [handler: handler, router: router]
    end

    test "the keyword registry config resolves to the adapter with its scope and failover delay range" do
      adapter = Commanded.Application.registry_adapter(CommandedApp)

      assert {SynRegistry,
              %{application: CommandedApp, scope: CommandedApp, failover_delay_range: {0, 0}}} =
               adapter
    end

    test "the event handler and the process router are registered under their syn names", ctx do
      %{handler: handler, router: router} = ctx

      assert is_pid(handler)
      assert is_pid(router)
      assert handler != router
    end

    test "the registered names resolve to the processes inside the supervision tree's singleton hosts",
         ctx do
      %{handler: handler, router: router} = ctx

      assert child_pid(tree_child_pid(EventHandler)) == handler
      assert child_pid(tree_child_pid(ProcessManager)) == router
    end

    test "starting the event handler again yields a proxy and leaves the registered handler in place",
         ctx do
      %{handler: handler} = ctx

      assert {:ok, host} = EventHandler.start_link()

      assert %SingletonProxy{pid: ^handler} = :sys.get_state(child_pid(host))
      assert Process.alive?(handler)

      assert Registration.whereis_name(CommandedApp, Handler.name(CommandedApp, @handler_name)) ==
               handler
    end

    test "stopping an event handler started a second time leaves the first one running and registered",
         ctx do
      %{handler: handler} = ctx
      {:ok, second_host} = EventHandler.start_link()

      :ok = GenServer.stop(second_host)

      assert Process.alive?(handler)

      assert Registration.whereis_name(CommandedApp, Handler.name(CommandedApp, @handler_name)) ==
               handler
    end

    test "an event handler that stops from error/3 ends its host with its own reason, and a temporary parent drops the host" do
      counter_id = "syn_registry_test_rejected_counter"
      handler_spec = Supervisor.child_spec(StoppingEventHandler, restart: :temporary)
      parent_start = {Supervisor, :start_link, [[handler_spec], [strategy: :one_for_one]]}
      parent = start_supervised!(%{id: :stopping_parent, start: parent_start, type: :supervisor})
      host = child_pid(parent)
      ref = Process.monitor(host)

      :ok = CommandedApp.dispatch(%Increment{counter_id: counter_id})

      assert_receive {:DOWN, ^ref, :process, ^host, reason}, 2_000
      assert reason == {:rejected, counter_id}
      assert Supervisor.which_children(parent) == []
    end

    # `Commanded.ProcessManagers.ProcessRouter` has no catch-all `handle_info/2`
    # clause; unmatched messages reach the fallback generated by
    # `use Commanded.Registration`, which calls the registry adapter.
    test "a process router ignores an unexpected message", ctx do
      %{router: router} = ctx

      send(router, :unexpected_message)

      assert GenServer.call(router, :process_instances) == []
    end

    test "a command dispatched while the aggregate stops normally is served by a new aggregate" do
      counter_id = "syn_registry_test_counter"
      aggregate_name = CommandedAggregate.name(CommandedApp, Counter, counter_id)

      slow_dispatch =
        Task.async(fn -> CommandedApp.dispatch(%IncrementSlowly{counter_id: counter_id}) end)

      counter = await_registered_counter(counter_id)
      # `GenServer.stop/2` blocks until the aggregate is done with the slow
      # command, so it runs in a task of its own. The pause lets its request
      # reach the mailbox ahead of the second command.
      stop = Task.async(fn -> GenServer.stop(counter, :normal) end)
      Process.sleep(50)

      assert CommandedApp.dispatch(%Increment{counter_id: counter_id}) == :ok

      assert Task.await(slow_dispatch) == :ok
      assert Task.await(stop) == :ok
      refute Process.alive?(counter)

      # Had the second command reached the aggregate before the stop, nothing
      # would have started a new one.
      replacement = Registration.whereis_name(CommandedApp, aggregate_name)
      assert is_pid(replacement)
      assert replacement != counter
    end
  end

  defp await_registered_counter(counter_id) do
    name = CommandedAggregate.name(CommandedApp, Counter, counter_id)

    await_registered(name, 100)
  end

  defp await_registered(_name, 0), do: flunk("the aggregate did not register itself in time")

  defp await_registered(name, attempts_left) do
    case Registration.whereis_name(CommandedApp, name) do
      :undefined ->
        Process.sleep(5)
        await_registered(name, attempts_left - 1)

      pid ->
        pid
    end
  end

  defp child_pid(supervisor) do
    [{_id, pid, _type, _modules}] = Supervisor.which_children(supervisor)

    pid
  end

  defp tree_child_pid(module) do
    children = Supervisor.which_children(CommandedTree)
    {_id, pid, _type, _modules} = Enum.find(children, &child_of_module?(&1, module))

    pid
  end

  defp child_of_module?({_id, _pid, _type, modules}, module), do: module in modules
end
