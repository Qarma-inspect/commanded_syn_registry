defmodule Commanded.Registration.SynRegistry.SingletonHostTest do
  use ExUnit.Case, async: false

  import Commanded.Registration.SynRegistry.SupervisorChildren
  import ExUnit.CaptureLog

  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.Polling
  alias Commanded.Registration.SynRegistry.RefusingProcess
  alias Commanded.Registration.SynRegistry.SingletonHost
  alias Commanded.Registration.SynRegistry.SingletonProxy
  alias Commanded.Registration.SynRegistry.SingletonReaper

  defmodule Singleton do
    use GenServer

    def init(state), do: {:ok, state}

    def handle_call(:ping, _from, state), do: {:reply, :pong, state}
  end

  defmodule UnstartableSingleton do
    use GenServer

    def init(_arg), do: {:stop, :boom}
  end

  # Tells the test process which host started it, and crashes `crash_after_ms`
  # later, the way a handler that fails on the same event does after every
  # start.
  defmodule CrashingSingleton do
    use GenServer

    def init({test_process, crash_after_ms}) do
      {:parent, host} = Process.info(self(), :parent)
      send(test_process, {:singleton_started, host})
      Process.send_after(self(), :crash, crash_after_ms)

      {:ok, nil}
    end

    def handle_info(:crash, state), do: {:stop, :crashed, state}
  end

  # Starts on its first start only; every later start fails, the way a handler
  # does that cannot rebuild its state after it lost its name.
  defmodule FailingRestartSingleton do
    use GenServer

    def init(starts) do
      case Agent.get_and_update(starts, fn count -> {count, count + 1} end) do
        0 -> {:ok, nil}
        _later -> {:stop, :boom}
      end
    end
  end

  # Traps exits, as a Commanded event handler does, and blocks in a callback
  # for as long as the test asks.
  defmodule BlockingSingleton do
    use GenServer

    def init(state) do
      Process.flag(:trap_exit, true)

      {:ok, state}
    end

    def handle_call({:block, block_ms}, from, state) do
      GenServer.reply(from, :blocking)
      Process.sleep(block_ms)

      {:noreply, state}
    end
  end

  # Traps exits, tells the test process it is starting, and takes `init_ms`
  # to finish its init/1, all while its name is already registered.
  defmodule SlowStartingSingleton do
    use GenServer

    def init({test_process, init_ms}) do
      Process.flag(:trap_exit, true)
      send(test_process, {:singleton_starting, self()})
      Process.sleep(init_ms)

      {:ok, nil}
    end
  end

  # Traps exits. At every start after the first, its init/1 suspends the host
  # before the host takes the start acknowledgement, and its
  # handle_continue/2 sends the test process its pid and blocks until it is
  # killed.
  defmodule SuspendingSingleton do
    use GenServer

    def init({test_process, starts}) do
      Process.flag(:trap_exit, true)

      case Agent.get_and_update(starts, &{&1, &1 + 1}) do
        0 -> {:ok, test_process}
        _restart -> suspend_host(test_process)
      end
    end

    def handle_continue(:block, test_process) do
      send(test_process, {:continuing, self()})
      Process.sleep(:infinity)

      {:noreply, test_process}
    end

    defp suspend_host(test_process) do
      {:parent, host} = Process.info(self(), :parent)
      true = :erlang.suspend_process(host)

      {:ok, test_process, {:continue, :block}}
    end
  end

  @scope __MODULE__.App
  @poll_timeout_ms 2_000
  @prompt_exit_ms 500
  @name_conflict {:shutdown, :name_conflict}

  setup do
    :ok = :syn.set_event_handler(ConflictResolution)
    :ok = :syn.add_node_to_scopes([@scope])

    adapter_meta = %{application: :adapter_application, scope: @scope, failover_delay_range: {0, 0}}

    name = {:handler, make_ref()}
    [adapter_meta: adapter_meta, name: name]
  end

  describe "start_link/5 when the name is free" do
    test "hosts the process registered under the name", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      {:ok, host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      child = fetch_child(host)
      assert :syn.whereis_name({@scope, name}) == child
      assert GenServer.call(child, :ping) == :pong
    end

    test "starts the process with the given arguments", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      {:ok, host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      singleton = fetch_child(host)

      assert :sys.get_state(singleton) == :state
    end

    test "registers the process with the time it was started", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      before_start = System.system_time(:nanosecond)

      {:ok, _host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      after_start = System.system_time(:nanosecond)
      {_pid, %{started_at: started_at}} = :syn.lookup(@scope, name)
      assert started_at in before_start..after_start
    end

    test "registers the process as a singleton", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      {:ok, _host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      assert {_pid, %{kind: :singleton}} = :syn.lookup(@scope, name)
    end

    test "passes the start options on to the process", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      {:ok, host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, spawn_opt: [priority: :high])

      singleton = fetch_child(host)

      assert Process.info(singleton, :priority) == {:priority, :high}
    end

    test "registers the process under the syn name even when the options carry another name", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx

      {:ok, host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, name: :ignored_name)

      child = fetch_child(host)
      assert :syn.whereis_name({@scope, name}) == child
      assert Process.whereis(:ignored_name) == nil
    end

    test "does not start when its process cannot be started", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      Process.flag(:trap_exit, true)

      start = SingletonHost.start_link(adapter_meta, name, UnstartableSingleton, :state, [])

      assert {:error, {:shutdown, {:failed_to_start_child, UnstartableSingleton, :boom}}} = start
    end
  end

  describe "start_link/5 when the name is taken" do
    setup ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      holder_host = start_host(adapter_meta, name)
      holder = fetch_child(holder_host)
      [holder_host: holder_host, holder: holder]
    end

    test "hosts a proxy for the process that holds the name", ctx do
      %{adapter_meta: adapter_meta, name: name, holder: holder} = ctx

      {:ok, host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      proxy = fetch_child(host)

      assert %SingletonProxy{pid: ^holder} = :sys.get_state(proxy)
    end

    test "does not start when the adapter metadata carries no scope", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      adapter_meta = Map.delete(adapter_meta, :scope)
      Process.flag(:trap_exit, true)

      start = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      assert {:error, {{:badkey, :scope}, _stacktrace}} = start
    end

    test "does not start when the adapter metadata carries no failover delay range", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      adapter_meta = Map.delete(adapter_meta, :failover_delay_range)
      Process.flag(:trap_exit, true)

      start = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      assert {:error, {{:badkey, :failover_delay_range}, _stacktrace}} = start
    end

    test "hands the proxy the syn name and the failover delay range", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      adapter_meta = Map.put(adapter_meta, :failover_delay_range, {1, 2})

      {:ok, host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      proxy_name = {:via, :syn, {@scope, name}}
      proxy = fetch_child(host)

      assert %SingletonProxy{name: ^proxy_name, failover_delay_range: {1, 2}} = :sys.get_state(proxy)
    end

    test "leaves the name resolving to the process that holds it", ctx do
      %{adapter_meta: adapter_meta, name: name, holder: holder} = ctx

      {:ok, _host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      assert :syn.whereis_name({@scope, name}) == holder
    end

    test "stopping it leaves the process that holds the name alive", ctx do
      %{adapter_meta: adapter_meta, name: name, holder: holder} = ctx
      {:ok, host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])

      :ok = GenServer.stop(host)

      assert Process.alive?(holder)
      assert GenServer.call(holder, :ping) == :pong
    end

    test "hosts the registered process itself once the holder is gone", ctx do
      %{adapter_meta: adapter_meta, name: name, holder_host: holder_host} = ctx
      {:ok, host} = SingletonHost.start_link(adapter_meta, name, Singleton, :state, [])
      proxy = fetch_child(host)

      :ok = GenServer.stop(holder_host)

      child = await_new_child(host, proxy)
      assert await_registered_pid(name, child) == child
      assert GenServer.call(child, :ping) == :pong
    end
  end

  describe "start_link/5 when syn refuses the name for a holder that has gone" do
    test "hosts the singleton itself once the refused registration has cleared", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      counter = RefusingProcess.start_counter(2)

      {:ok, host} = SingletonHost.start_link(adapter_meta, name, RefusingProcess, counter, [])

      child = fetch_child(host)
      assert :syn.whereis_name({@scope, name}) == child
      assert RefusingProcess.fetch_start_count(counter) == 3
    end

    test "does not start once five attempts are refused", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      counter = RefusingProcess.start_counter(100)
      Process.flag(:trap_exit, true)

      start = SingletonHost.start_link(adapter_meta, name, RefusingProcess, counter, [])

      refusal = {:already_started, :undefined}
      assert start == {:error, {:shutdown, {:failed_to_start_child, RefusingProcess, refusal}}}
      assert RefusingProcess.fetch_start_count(counter) == 5
    end
  end

  describe "Supervisor calls" do
    test "Supervisor.which_children/1 reports the registered process as its one worker", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_host(adapter_meta, name)
      singleton = :syn.whereis_name({@scope, name})

      assert Supervisor.which_children(host) == [{Singleton, singleton, :worker, [Singleton]}]
    end

    test "Supervisor.which_children/1 reports a proxy as its one worker", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      _holder_host = start_host(adapter_meta, name)
      host = start_host(adapter_meta, name)

      assert [{Singleton, proxy, :worker, [SingletonProxy]}] = Supervisor.which_children(host)
      assert %SingletonProxy{} = :sys.get_state(proxy)
    end

    test "Supervisor.count_children/1 counts one active worker", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_host(adapter_meta, name)

      assert Supervisor.count_children(host) == %{specs: 1, active: 1, supervisors: 0, workers: 1}
    end

    test "answers any other call with an error and keeps hosting the singleton", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_host(adapter_meta, name)
      singleton = fetch_child(host)

      assert Supervisor.terminate_child(host, Singleton) == {:error, :not_supported}
      assert GenServer.call(host, :unknown_request) == {:error, :not_supported}

      assert fetch_child(host) == singleton
      assert :syn.whereis_name({@scope, name}) == singleton
    end
  end

  describe "messages that are not its own" do
    test "keeps hosting the singleton after a message it does not know", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_unlinked_host(adapter_meta, name)
      singleton = fetch_child(host)
      host_ref = Process.monitor(host)

      send(host, :stray)

      refute_receive {:DOWN, ^host_ref, :process, ^host, _reason}, @prompt_exit_ms
      assert fetch_child(host) == singleton
    end

    test "keeps hosting the singleton after an exit signal from a process that is not its child", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_unlinked_host(adapter_meta, name)
      singleton = fetch_child(host)
      host_ref = Process.monitor(host)

      spawn(Process, :link, [host])

      refute_receive {:DOWN, ^host_ref, :process, ^host, _reason}, @prompt_exit_ms
      assert fetch_child(host) == singleton
    end
  end

  describe "the singleton's own exits" do
    test "a transient parent leaves the singleton stopped after it stops normally", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :transient)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      :ok = host |> fetch_child() |> GenServer.stop(:normal)

      assert_receive {:DOWN, ^ref, :process, ^host, :normal}, @poll_timeout_ms
      assert [{:singleton, :undefined, :worker, _modules}] = Supervisor.which_children(parent)
      assert :syn.whereis_name({@scope, name}) == :undefined
    end

    test "a temporary parent drops the singleton after it crashes", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      host |> fetch_child() |> Process.exit(:crashed)

      assert_receive {:DOWN, ^ref, :process, ^host, :crashed}, @poll_timeout_ms
      assert Supervisor.which_children(parent) == []
      assert :syn.whereis_name({@scope, name}) == :undefined
    end

    test "a killed singleton ends its host with :killed", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      host |> fetch_child() |> Process.exit(:kill)

      assert_receive {:DOWN, ^ref, :process, ^host, :killed}, @poll_timeout_ms
      assert Supervisor.which_children(parent) == []
    end

    test "a singleton that stops with :kill ends its host with :kill", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      :ok = host |> fetch_child() |> GenServer.stop(:kill)

      assert_receive {:DOWN, ^ref, :process, ^host, :kill}, @poll_timeout_ms
      assert Supervisor.which_children(parent) == []
    end

    test "a singleton that stops with {:shutdown, reason} of its own ends its host with that reason", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      :ok = host |> fetch_child() |> GenServer.stop({:shutdown, :rejected})

      assert_receive {:DOWN, ^ref, :process, ^host, {:shutdown, :rejected}}, @poll_timeout_ms
      assert Supervisor.which_children(parent) == []
    end

    test "a singleton that stops with :shutdown ends its host with :shutdown", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      :ok = host |> fetch_child() |> GenServer.stop(:shutdown)

      assert_receive {:DOWN, ^ref, :process, ^host, :shutdown}, @poll_timeout_ms
      assert Supervisor.which_children(parent) == []
    end

    test "a permanent parent starts a new host after the singleton crashes", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :permanent)
      host = fetch_child(parent)

      host |> fetch_child() |> Process.exit(:crashed)

      new_host = await_new_child(parent, host)
      singleton = fetch_child(new_host)
      assert await_registered_pid(name, singleton) == singleton
    end

    test "every crash of a singleton that crashes 300 ms after each start reaches the parent", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      args = {self(), 300}

      _parent = start_parent(adapter_meta, name, CrashingSingleton, args, restart: :permanent)

      hosts = Enum.map(1..3, fn _start -> receive_singleton_start() end)
      distinct_hosts = Enum.uniq(hosts)
      assert length(distinct_hosts) == 3
    end

    test "logs no crash report of the host for the singleton's exit", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      log =
        capture_log(fn ->
          host |> fetch_child() |> Process.exit(:crashed)
          assert_receive {:DOWN, ^ref, :process, ^host, :crashed}, @poll_timeout_ms
          Process.sleep(100)
        end)

      refute log =~ "GenServer #{inspect(host)} terminating"
    end

    test "logs no crash report of the host for a singleton that stops with :kill", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      log =
        capture_log(fn ->
          :ok = host |> fetch_child() |> GenServer.stop(:kill)
          assert_receive {:DOWN, ^ref, :process, ^host, :kill}, @poll_timeout_ms
          Process.sleep(100)
        end)

      refute log =~ "GenServer #{inspect(host)} terminating"
    end
  end

  describe "registry churn while syn refuses the name for a holder that has gone" do
    test "keeps the host through a restart whose five starts are refused, and hosts the singleton once the name clears", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      {host, counter} = start_host_refused_after_first_start(adapter_meta, name, 5)
      singleton = fetch_child(host)
      ref = Process.monitor(host)

      :ok = GenServer.stop(singleton, @name_conflict)

      replacement = await_new_child(host, singleton)
      assert await_registered_pid(name, replacement) == replacement
      assert RefusingProcess.fetch_start_count(counter) == 7
      refute_received {:DOWN, ^ref, :process, ^host, _reason}
    end

    @tag :capture_log
    test "counts each refused restart against the budget and stops with too many registry restarts", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      {host, _counter} = start_host_refused_after_first_start(adapter_meta, name, 1_000)
      ref = Process.monitor(host)

      :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)

      assert_receive {:DOWN, ^ref, :process, ^host, reason}, 5_000
      assert reason == {:too_many_registry_restarts, name}
    end

    test "reports its worker as restarting to Supervisor calls between refused restarts", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      {host, _counter} = start_host_refused_after_first_start(adapter_meta, name, 1_000)

      :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)

      assert await_restarting_child(host) == [{RefusingProcess, :restarting, :worker, [RefusingProcess]}]

      assert Supervisor.count_children(host) == %{specs: 1, active: 0, supervisors: 0, workers: 1}
    end

    test "stops when asked to between refused restarts", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      {host, _counter} = start_host_refused_after_first_start(adapter_meta, name, 1_000)
      :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)
      await_restarting_child(host)

      assert GenServer.stop(host) == :ok
    end
  end

  describe "stopping by the parent" do
    test "a parent whose 1 second shutdown runs out finds the singleton blocked in a callback killed and its name free", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, BlockingSingleton, :state, shutdown: 1_000)
      singleton = parent |> fetch_child() |> fetch_child()
      :blocking = GenServer.call(singleton, {:block, 3_000})
      ref = Process.monitor(singleton)

      :ok = Supervisor.stop(parent)

      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @poll_timeout_ms
      assert await_registered_pid(name, :undefined) == :undefined
    end

    test "a parent with :brutal_kill finds the singleton blocked in a callback killed and its name free", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, BlockingSingleton, :state, shutdown: :brutal_kill)
      singleton = parent |> fetch_child() |> fetch_child()
      :blocking = GenServer.call(singleton, {:block, 3_000})
      ref = Process.monitor(singleton)

      :ok = Supervisor.stop(parent)

      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @poll_timeout_ms
      assert await_registered_pid(name, :undefined) == :undefined
    end

    test "parents that wait 10 seconds or without a limit let a singleton blocked for 5.5 seconds finish and stop with :shutdown",
         ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      other_name = {:handler, make_ref()}

      waiting_parent = start_parent(adapter_meta, name, BlockingSingleton, :state, shutdown: 10_000, parent_id: :waiting)

      patient_parent = start_parent(adapter_meta, other_name, BlockingSingleton, :state, shutdown: :infinity, parent_id: :patient)

      parents = [waiting_parent, patient_parent]
      [waiting, patient] = Enum.map(parents, &(&1 |> fetch_child() |> fetch_child()))
      :blocking = GenServer.call(waiting, {:block, 5_500})
      :blocking = GenServer.call(patient, {:block, 5_500})
      waiting_ref = Process.monitor(waiting)
      patient_ref = Process.monitor(patient)

      stops = Enum.map(parents, &Task.async(Supervisor, :stop, [&1]))

      assert_receive {:DOWN, ^waiting_ref, :process, ^waiting, :shutdown}, 7_000
      assert_receive {:DOWN, ^patient_ref, :process, ^patient, :shutdown}, 7_000
      assert Task.await_many(stops) == [:ok, :ok]
    end

    test "a parent whose 1 second shutdown runs out finds the singleton blocked in a callback killed and its name free when the reaper was killed during the wait",
         ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, BlockingSingleton, :state, shutdown: 1_000)
      host = fetch_child(parent)
      singleton = fetch_child(host)
      reaper = :sys.get_state(host).reaper
      :blocking = GenServer.call(singleton, {:block, 5_000})
      ref = Process.monitor(singleton)
      stop = Task.async(Supervisor, :stop, [parent])
      await_stop_request(singleton, host)

      Process.exit(reaper, :kill)

      assert Task.await(stop) == :ok
      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @poll_timeout_ms
      assert await_registered_pid(name, :undefined) == :undefined
    end

    test "a parent whose 1 second shutdown runs out finds the singleton blocked in a callback killed and its name free when two reapers in a row were killed during the wait",
         ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, BlockingSingleton, :state, shutdown: 1_000)
      host = fetch_child(parent)
      singleton = fetch_child(host)
      reaper = :sys.get_state(host).reaper
      :blocking = GenServer.call(singleton, {:block, 5_000})
      ref = Process.monitor(singleton)
      stop = Task.async(Supervisor, :stop, [parent])
      await_stop_request(singleton, host)
      Process.exit(reaper, :kill)
      [new_reaper] = await_other_processes_started_by(host, [singleton, reaper])

      Process.exit(new_reaper, :kill)

      assert Task.await(stop) == :ok
      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @poll_timeout_ms
      assert await_registered_pid(name, :undefined) == :undefined
    end

    test "a parent whose 1 second shutdown runs out finds the singleton blocked in a callback killed when the reaper was killed during the wait and the name no longer resolves to the singleton",
         ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, BlockingSingleton, :state, shutdown: 1_000)
      host = fetch_child(parent)
      singleton = fetch_child(host)
      reaper = :sys.get_state(host).reaper
      :blocking = GenServer.call(singleton, {:block, 5_000})
      # With the name gone, only the pid the host hands its new reaper leads to the singleton.
      :ok = :syn.unregister(@scope, name)
      ref = Process.monitor(singleton)
      stop = Task.async(Supervisor, :stop, [parent])
      await_stop_request(singleton, host)

      Process.exit(reaper, :kill)

      assert Task.await(stop) == :ok
      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @poll_timeout_ms
    end

    # Loading the module again drops the coverage instrumentation, so a
    # coverage run excludes this test.
    @tag :code_reload
    test "a parent whose 1 second shutdown runs out finds the singleton blocked in a callback killed and its name free when a code purge killed the reaper during the wait",
         ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, BlockingSingleton, :state, shutdown: 1_000)
      host = fetch_child(parent)
      singleton = fetch_child(host)
      :blocking = GenServer.call(singleton, {:block, 5_000})
      ref = Process.monitor(singleton)
      stop = Task.async(Supervisor, :stop, [parent])
      await_stop_request(singleton, host)

      {:module, SingletonReaper} = :code.load_file(SingletonReaper)
      assert :code.purge(SingletonReaper)

      assert Task.await(stop) == :ok
      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @poll_timeout_ms
      assert await_registered_pid(name, :undefined) == :undefined
    end
  end

  describe "a killed host" do
    test "takes down a singleton that holds the name and is still starting", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      args = {self(), 2_000}
      spawn(fn -> start_host(adapter_meta, name, SlowStartingSingleton, args) end)
      assert_receive {:singleton_starting, singleton}, @poll_timeout_ms
      assert :syn.whereis_name({@scope, name}) == singleton
      {:parent, host} = Process.info(singleton, :parent)
      ref = Process.monitor(singleton)

      Process.exit(host, :kill)

      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @prompt_exit_ms
      assert await_registered_pid(name, :undefined) == :undefined
    end

    test "takes down its singleton blocked in a callback even when the name no longer resolves to it", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_unlinked_host(adapter_meta, name, BlockingSingleton)
      singleton = fetch_child(host)
      :blocking = GenServer.call(singleton, {:block, 3_000})
      :ok = :syn.unregister(@scope, name)
      ref = Process.monitor(singleton)

      Process.exit(host, :kill)

      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @prompt_exit_ms
    end

    test "takes down a singleton that lost its name before the host took its start acknowledgement", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_suspending_host(adapter_meta, name)
      :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)
      singleton = receive_continuing_singleton()
      :ok = :syn.unregister(@scope, name)
      ref = Process.monitor(singleton)

      Process.exit(host, :kill)

      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @prompt_exit_ms
    end

    test "takes down a singleton before the host took its start acknowledgement when syn's tables are gone", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_suspending_host(adapter_meta, name)
      :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)
      singleton = receive_continuing_singleton()
      crash_syn_backbone()
      ref = Process.monitor(singleton)

      Process.exit(host, :kill)

      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @prompt_exit_ms
    end

    test "takes down a singleton that lost its name while starting and leaves alone the singleton of another host that took the name over",
         ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_suspending_host(adapter_meta, name)
      reaper = :sys.get_state(host).reaper
      reaper_ref = Process.monitor(reaper)
      :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)
      singleton = receive_continuing_singleton()
      :ok = :syn.unregister(@scope, name)
      other_singleton = adapter_meta |> start_host(name) |> fetch_child()
      ref = Process.monitor(singleton)

      Process.exit(host, :kill)

      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @prompt_exit_ms
      assert_receive {:DOWN, ^reaper_ref, :process, ^reaper, :normal}, @poll_timeout_ms
      assert Process.alive?(other_singleton)
      assert :syn.whereis_name({@scope, name}) == other_singleton
    end

    test "takes down a singleton it started after its reaper died and before it read the reaper's DOWN", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_suspending_host(adapter_meta, name)
      first_singleton = fetch_child(host)
      reaper = :sys.get_state(host).reaper
      reaper_ref = Process.monitor(reaper)
      true = :erlang.suspend_process(host)
      # The host reads the singleton's exit before the reaper's DOWN.
      :ok = GenServer.stop(first_singleton, @name_conflict)
      Process.exit(reaper, :kill)
      assert_receive {:DOWN, ^reaper_ref, :process, ^reaper, :killed}, @poll_timeout_ms
      true = :erlang.resume_process(host)
      singleton = receive_continuing_singleton()
      ref = Process.monitor(singleton)

      Process.exit(host, :kill)

      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @prompt_exit_ms
    end

    test "that proxies leaves the process holding the name alive", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      holder = adapter_meta |> start_host(name) |> fetch_child()
      host = start_unlinked_host(adapter_meta, name)
      reaper = :sys.get_state(host).reaper
      reaper_ref = Process.monitor(reaper)

      Process.exit(host, :kill)

      assert_receive {:DOWN, ^reaper_ref, :process, ^reaper, :normal}, @poll_timeout_ms
      assert Process.alive?(holder)
      assert :syn.whereis_name({@scope, name}) == holder
    end

    test "takes down its singleton after the reaper that guarded it was killed", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_unlinked_host(adapter_meta, name, BlockingSingleton)
      singleton = fetch_child(host)
      reaper = :sys.get_state(host).reaper

      Process.exit(reaper, :kill)

      await_new_reaper(host, reaper)
      # With the name gone, only the pid the host hands its new reaper leads to the singleton.
      :ok = :syn.unregister(@scope, name)
      :blocking = GenServer.call(singleton, {:block, 3_000})
      ref = Process.monitor(singleton)
      Process.exit(host, :kill)
      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @prompt_exit_ms
    end

    # Loading the module again drops the coverage instrumentation, so a
    # coverage run excludes this test.
    @tag :code_reload
    test "takes down its singleton after a code purge killed the reaper that guarded it", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_unlinked_host(adapter_meta, name, BlockingSingleton)
      singleton = fetch_child(host)
      reaper = :sys.get_state(host).reaper

      {:module, SingletonReaper} = :code.load_file(SingletonReaper)
      assert :code.purge(SingletonReaper)

      await_new_reaper(host, reaper)
      :blocking = GenServer.call(singleton, {:block, 3_000})
      ref = Process.monitor(singleton)
      Process.exit(host, :kill)
      assert_receive {:DOWN, ^ref, :process, ^singleton, :killed}, @prompt_exit_ms
    end
  end

  describe "registry churn" do
    test "a temporary parent keeps the host through a lost conflict, and the host registers the singleton again", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      singleton = fetch_child(host)

      :ok = GenServer.stop(singleton, @name_conflict)

      replacement = await_new_child(host, singleton)
      assert await_registered_pid(name, replacement) == replacement
      assert fetch_child(parent) == host
    end

    test "runs only the new singleton and its first reaper after a lost conflict", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_host(adapter_meta, name)
      reaper = :sys.get_state(host).reaper

      singleton = lose_conflict_and_await_restart(host)

      started_by_host = Enum.filter(Process.list(), &(Process.info(&1, :parent) == {:parent, host}))

      assert Enum.sort(started_by_host) == Enum.sort([singleton, reaper])
    end

    test "a start error at a restart ends the host with the error", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      starts = start_supervised!({Agent, fn -> 0 end})

      parent = start_parent(adapter_meta, name, FailingRestartSingleton, starts, restart: :temporary)

      host = fetch_child(parent)
      host_ref = Process.monitor(host)

      :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)

      assert_receive {:DOWN, ^host_ref, :process, ^host, :boom}, @poll_timeout_ms
      assert Supervisor.which_children(parent) == []
    end

    test "a crash of the holder ends the proxying host with the crash reason, and a temporary parent drops it", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      holder_parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary, parent_id: :holder)
      holder = holder_parent |> fetch_child() |> fetch_child()
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      Process.exit(holder, :crashed)

      assert_receive {:DOWN, ^ref, :process, ^host, :crashed}, @poll_timeout_ms
      assert Supervisor.which_children(parent) == []
      assert await_registered_pid(name, :undefined) == :undefined
    end

    test "a holder that stops with :kill ends the proxying host with :kill, and a temporary parent drops it", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      holder_parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary, parent_id: :holder)
      holder = holder_parent |> fetch_child() |> fetch_child()
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      ref = Process.monitor(host)

      :ok = GenServer.stop(holder, :kill)

      assert_receive {:DOWN, ^ref, :process, ^host, :kill}, @poll_timeout_ms
      assert Supervisor.which_children(parent) == []
      assert await_registered_pid(name, :undefined) == :undefined
    end

    test "a temporary parent keeps the proxying host when the holder is stopped with :shutdown, and that host takes the name over",
         ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      holder_parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary, parent_id: :holder)
      holder = holder_parent |> fetch_child() |> fetch_child()
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      proxy = fetch_child(host)

      :ok = GenServer.stop(holder, :shutdown)

      singleton = await_new_child(host, proxy)
      assert await_registered_pid(name, singleton) == singleton
      assert fetch_child(parent) == host
      assert Supervisor.which_children(holder_parent) == []
    end

    test "a temporary parent keeps the proxying host when the holder is killed, and that host takes the name over", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      holder_parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary, parent_id: :holder)
      holder = holder_parent |> fetch_child() |> fetch_child()
      parent = start_parent(adapter_meta, name, Singleton, :state, restart: :temporary)
      host = fetch_child(parent)
      proxy = fetch_child(host)

      Process.exit(holder, :kill)

      singleton = await_new_child(host, proxy)
      assert await_registered_pid(name, singleton) == singleton
      assert fetch_child(parent) == host
      assert Supervisor.which_children(holder_parent) == []
    end

    test "absorbs thirty lost conflicts in five seconds and stops on the thirty-first, logging an error that names the singleton",
         ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_unlinked_host(adapter_meta, name)
      ref = Process.monitor(host)

      Enum.each(1..30, fn _loss -> lose_conflict_and_await_restart(host) end)
      refute_received {:DOWN, ^ref, :process, ^host, _reason}

      log =
        capture_log(fn ->
          :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)
          assert_receive {:DOWN, ^ref, :process, ^host, reason}, @poll_timeout_ms
          assert reason == {:too_many_registry_restarts, name}
        end)

      assert log =~ "[error]"
      assert log =~ "name=#{inspect(name)}"
    end

    test "counts the lost conflicts of the last five seconds", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_unlinked_host(adapter_meta, name)
      ref = Process.monitor(host)
      first_loss_at = System.monotonic_time(:millisecond)

      Enum.each(1..30, fn _loss -> lose_conflict_and_await_restart(host) end)
      sleep_until(first_loss_at + 4_500)
      :ok = host |> fetch_child() |> GenServer.stop(@name_conflict)

      assert_receive {:DOWN, ^ref, :process, ^host, {:too_many_registry_restarts, ^name}}, @poll_timeout_ms
    end

    test "forgets lost conflicts older than five seconds", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      host = start_unlinked_host(adapter_meta, name)
      ref = Process.monitor(host)

      Enum.each(1..30, fn _loss -> lose_conflict_and_await_restart(host) end)
      Process.sleep(5_100)
      lose_conflict_and_await_restart(host)

      refute_received {:DOWN, ^ref, :process, ^host, _reason}
      assert Process.alive?(host)
    end

    test "logs each absorbed restart at debug level with the name and the reason", ctx do
      %{adapter_meta: adapter_meta, name: name} = ctx
      previous_level = Logger.level()
      Logger.configure(level: :debug)
      on_exit(fn -> Logger.configure(level: previous_level) end)
      host = start_host(adapter_meta, name)

      log = capture_log(fn -> lose_conflict_and_await_restart(host) end)

      assert log =~ "[debug]"
      assert log =~ "name=#{inspect(name)}"
      assert log =~ "reason=#{inspect(@name_conflict)}"
      refute log =~ "[info]"
    end
  end

  # A host running a `RefusingProcess` that syn refuses on every start after
  # the first.
  defp start_host_refused_after_first_start(adapter_meta, name, refusals) do
    counter = RefusingProcess.start_counter(0)
    host = start_unlinked_host(adapter_meta, name, RefusingProcess, counter)
    :ok = RefusingProcess.refuse_next_starts(counter, refusals)

    {host, counter}
  end

  defp start_host(adapter_meta, name, module \\ Singleton, args \\ :state) do
    {:ok, host} = SingletonHost.start_link(adapter_meta, name, module, args, [])

    host
  end

  # For a host that is expected to exit with an abnormal reason, which would
  # take a linked test process with it.
  defp start_unlinked_host(adapter_meta, name, module \\ Singleton, args \\ :state) do
    host = start_host(adapter_meta, name, module, args)
    Process.unlink(host)
    on_exit(fn -> Process.exit(host, :kill) end)

    host
  end

  # A one-for-one supervisor with the host as its only child. `child_overrides`
  # go into the host's child spec, except `:parent_id`, which tells two
  # parents in one test apart.
  defp start_parent(adapter_meta, name, module, args, child_overrides) do
    {parent_id, child_overrides} = Keyword.pop(child_overrides, :parent_id, :parent)
    host_start = {SingletonHost, :start_link, [adapter_meta, name, module, args, []]}
    host_spec = Supervisor.child_spec(%{id: :singleton, start: host_start}, child_overrides)

    start_supervised!(%{
      id: parent_id,
      start: {Supervisor, :start_link, [[host_spec], [strategy: :one_for_one]]},
      restart: :temporary,
      type: :supervisor
    })
  end

  defp start_suspending_host(adapter_meta, name) do
    starts = start_supervised!({Agent, fn -> 0 end})

    start_unlinked_host(adapter_meta, name, SuspendingSingleton, {self(), starts})
  end

  # The singleton blocks until it is killed, so the test kills it on exit in
  # case the host's reaper did not.
  defp receive_continuing_singleton do
    assert_receive {:continuing, singleton}, @poll_timeout_ms
    on_exit(fn -> Process.exit(singleton, :kill) end)

    singleton
  end

  # Kills `syn_backbone`, which owns syn's tables, and keeps it down until the
  # test ends by suspending the supervisor that would restart it.
  defp crash_syn_backbone do
    :ok = :sys.suspend(:syn_sup)
    on_exit(&restore_syn/0)
    backbone = Process.whereis(:syn_backbone)
    ref = Process.monitor(backbone)

    Process.exit(backbone, :kill)

    assert_receive {:DOWN, ^ref, :process, ^backbone, :killed}, @poll_timeout_ms
  end

  # The call returns once the supervisor has restarted `syn_backbone` and the
  # scopes after it, so the next test finds syn whole.
  defp restore_syn do
    :ok = :sys.resume(:syn_sup)
    _children = Supervisor.which_children(:syn_sup)
  end

  defp receive_singleton_start do
    assert_receive {:singleton_started, host}, @poll_timeout_ms

    host
  end

  defp lose_conflict_and_await_restart(host) do
    singleton = fetch_child(host)
    :ok = GenServer.stop(singleton, @name_conflict)

    await_new_child(host, singleton)
  end

  defp sleep_until(monotonic_ms) do
    remaining_ms = monotonic_ms - System.monotonic_time(:millisecond)

    Process.sleep(max(remaining_ms, 0))
  end

  defp await_new_child(supervisor, previous_child) do
    Polling.await(@poll_timeout_ms, fn ->
      child = fetch_child(supervisor)

      if is_pid(child) and child != previous_child,
        do: {:ok, child},
        else: {:error, "#{inspect(supervisor)} did not start a new child in time"}
    end)
  end

  defp await_restarting_child(host) do
    Polling.await(@poll_timeout_ms, fn ->
      children = Supervisor.which_children(host)

      if match?([{_id, :restarting, _type, _modules}], children),
        do: {:ok, children},
        else: {:error, "#{inspect(host)} did not report a restarting child in time"}
    end)
  end

  defp await_new_reaper(host, previous_reaper) do
    Polling.await(@poll_timeout_ms, fn ->
      reaper = :sys.get_state(host).reaper

      if is_pid(reaper) and reaper != previous_reaper,
        do: {:ok, reaper},
        else: {:error, "#{inspect(host)} did not start a new reaper in time"}
    end)
  end

  # Returns once the host has asked the blocked singleton to stop and waits
  # for it. The singleton traps exits, so the request stays in its mailbox.
  defp await_stop_request(singleton, host) do
    Polling.await(@poll_timeout_ms, fn ->
      {:messages, messages} = Process.info(singleton, :messages)

      if {:EXIT, host, :shutdown} in messages,
        do: {:ok, :ok},
        else: {:error, "#{inspect(host)} did not ask #{inspect(singleton)} to stop in time"}
    end)
  end

  # While the host waits for its child to stop it answers no `:sys` call, so
  # the test finds a new reaper among the processes the host started.
  defp await_other_processes_started_by(host, known_pids) do
    Polling.await(@poll_timeout_ms, fn ->
      started_by_host = Enum.filter(Process.list(), &(Process.info(&1, :parent) == {:parent, host}))
      other_pids = started_by_host -- known_pids

      if other_pids != [],
        do: {:ok, other_pids},
        else: {:error, "#{inspect(host)} started no new process in time"}
    end)
  end

  defp await_registered_pid(name, expected_pid) do
    Polling.await(@poll_timeout_ms, fn ->
      registered = :syn.whereis_name({@scope, name})

      if registered == expected_pid,
        do: {:ok, registered},
        else: {:error, "#{inspect(name)} resolves to #{inspect(registered)}"}
    end)
  end
end
