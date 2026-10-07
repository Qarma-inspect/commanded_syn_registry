defmodule Commanded.Registration.SynRegistry.ConflictResolutionTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Commanded.Registration.SynRegistry
  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.RecordingEventHandler

  defmodule Singleton do
    use GenServer

    def init(arg), do: {:ok, arg}
  end

  # A host's event handler that leaves registry conflicts to syn. It hears of
  # unregistrations and tells the process that lost the name, and it hears of
  # updates through the older of the two update callbacks only.
  defmodule ListeningEventHandler do
    @behaviour :syn_event_handler

    @impl :syn_event_handler
    def on_process_unregistered(scope, name, pid, metadata, reason) do
      send(pid, {:unregistered, name})

      report(:on_process_unregistered, [scope, name, pid, metadata, reason])
    end

    @impl :syn_event_handler
    def on_registry_process_updated(scope, name, pid, metadata, reason),
      do: report(:on_registry_process_updated, [scope, name, pid, metadata, reason])

    defp report(callback, arguments), do: send(__MODULE__, {__MODULE__, callback, arguments})
  end

  # The scope of a Commanded application, which the adapter creates.
  @scope __MODULE__.App
  # A scope of the host application, which the adapter knows nothing about.
  @host_scope :conflict_resolution_test_host_scope
  @name {:handler, "conflicted"}
  @group :conflict_resolution_test_group

  # Each test starts from a node where the adapter was installed over no other
  # syn event handler, and leaves the node that way.
  setup do
    :ok = install_adapter_over(nil)
    on_exit(fn -> install_adapter_over(nil) end)
    older = registry_entry(1_000)
    newer = registry_entry(2_000)
    [older: older, newer: newer]
  end

  describe "resolve_registry_conflict/4" do
    test "keeps the process that was started first", ctx do
      %{older: older, newer: newer} = ctx

      assert ConflictResolution.resolve_registry_conflict(@scope, @name, older, newer) == pid_of(older)
    end

    test "keeps the same process whichever side of the conflict it arrives from", ctx do
      %{older: older, newer: newer} = ctx

      kept = ConflictResolution.resolve_registry_conflict(@scope, @name, newer, older)

      assert kept == ConflictResolution.resolve_registry_conflict(@scope, @name, older, newer)
    end

    test "keeps the same process for two processes started at the same time", ctx do
      %{older: older} = ctx
      other = registry_entry(1_000)

      kept = ConflictResolution.resolve_registry_conflict(@scope, @name, older, other)

      assert kept == max(pid_of(older), pid_of(other))
      assert ConflictResolution.resolve_registry_conflict(@scope, @name, other, older) == kept
    end
  end

  describe "on_process_unregistered/5" do
    test "stops a local singleton that lost a conflict with the reason {:shutdown, :name_conflict}" do
      pid = start_singleton()
      ref = Process.monitor(pid)
      metadata = build_registration_metadata(1_000, :singleton)

      capture_log(fn -> assert unregister_after_conflict(pid, metadata) == :ok end)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :name_conflict}}
    end

    test "stops a local aggregate that lost a conflict with the reason :normal" do
      pid = start_singleton()
      ref = Process.monitor(pid)
      metadata = build_registration_metadata(1_000, :aggregate)

      capture_log(fn -> assert unregister_after_conflict(pid, metadata) == :ok end)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "stops a local process registered without a kind with the reason :normal" do
      pid = start_singleton()
      ref = Process.monitor(pid)
      metadata = build_started_at_metadata(1_000)

      capture_log(fn -> assert unregister_after_conflict(pid, metadata) == :ok end)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "reports no crash when the process that lost the conflict is already gone" do
      dead = spawn_dead_process()

      log = capture_log(fn -> unregister_after_conflict_and_settle(dead) end)

      assert log =~ "SynRegistry: conflict lost"
      refute log =~ "[error]"
    end

    test "logs the name that lost the conflict and the process being stopped" do
      pid = start_singleton()

      log = capture_log(fn -> unregister_after_conflict(pid) end)

      expected =
        "[info] SynRegistry: conflict lost, stopping the process: " <>
          "scope=#{inspect(@scope)} name=#{inspect(@name)} pid=#{inspect(pid)}"

      assert log =~ expected
    end

    test "leaves a process on another node to that node, with or without a kind in its metadata" do
      remote = build_pid_on_unreachable_node(:"unreachable@127.0.0.1")

      for metadata <- [build_registration_metadata(1_000, :singleton), build_started_at_metadata(1_000)] do
        log = capture_log(fn -> unregister_after_conflict_and_settle(remote, metadata) end)

        assert unregister_after_conflict(remote, metadata) == :ok
        refute log =~ "conflict lost"
        refute log =~ "[error]"
      end
    end

    test "leaves a process alone when it is unregistered for another reason" do
      pid = start_singleton()
      ref = Process.monitor(pid)
      metadata = build_started_at_metadata(1_000)

      assert ConflictResolution.on_process_unregistered(@scope, @name, pid, metadata, :normal) == :ok

      assert ConflictResolution.on_process_unregistered(
               @scope,
               @name,
               pid,
               metadata,
               {:syn_remote_scope_node_down, @scope, node()}
             ) == :ok

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 50
      assert Process.alive?(pid)
    end
  end

  describe "adapter_scope?/1" do
    test "is true for the scope of every application the adapter was started for" do
      {:ok, [], _adapter_meta} = SynRegistry.child_spec(__MODULE__.OtherApp, [])

      assert ConflictResolution.adapter_scope?(@scope)
      assert ConflictResolution.adapter_scope?(__MODULE__.OtherApp)
    end

    test "is false for a scope the adapter did not create" do
      refute ConflictResolution.adapter_scope?(@host_scope)
    end
  end

  describe "resolve_registry_conflict/4 in a scope of the host" do
    test "keeps the registration made last, as syn does, whatever the metadata holds" do
      first = host_entry(%{started_at: 2_000}, 1_000)
      last = host_entry(:undefined, 2_000)

      assert ConflictResolution.resolve_registry_conflict(@host_scope, @name, first, last) == pid_of(last)

      assert ConflictResolution.resolve_registry_conflict(@host_scope, @name, last, first) == pid_of(last)
    end

    test "keeps the greater pid for two registrations made at the same time, as syn does" do
      entry = host_entry(:undefined, 1_000)
      other = host_entry(:undefined, 1_000)
      greater_pid = max(pid_of(entry), pid_of(other))

      assert ConflictResolution.resolve_registry_conflict(@host_scope, @name, entry, other) == greater_pid

      assert ConflictResolution.resolve_registry_conflict(@host_scope, @name, other, entry) == greater_pid
    end
  end

  describe "on_process_unregistered/5 in a scope of the host" do
    test "sends a local process that lost a conflict the exit signal syn sends" do
      pid = spawn_plain_process()
      ref = Process.monitor(pid)

      {result, log} = with_log(fn -> unregister_in_host_scope(pid, :syn_conflict_resolution) end)

      assert result == :ok
      assert_receive {:DOWN, ^ref, :process, ^pid, {:syn_resolve_kill, @name, :metadata}}
      refute log =~ "conflict lost"
    end

    test "leaves a process alone when it is unregistered for another reason" do
      pid = spawn_plain_process()
      ref = Process.monitor(pid)

      assert unregister_in_host_scope(pid, :normal) == :ok

      assert unregister_in_host_scope(pid, {:syn_remote_scope_node_down, @host_scope, node()}) == :ok

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 50
    end
  end

  describe "an event handler installed before the adapter" do
    setup do
      Process.register(self(), RecordingEventHandler)
      :ok = install_adapter_over(RecordingEventHandler)
    end

    test "resolves the conflicts of the host's scopes" do
      first = host_entry(:undefined, 1_000)
      last = host_entry(:undefined, 2_000)

      assert ConflictResolution.resolve_registry_conflict(@host_scope, @name, last, first) == pid_of(first)

      assert_received {RecordingEventHandler, :resolve_registry_conflict, [@host_scope, @name, ^last, ^first]}
    end

    test "hears of the process that lost a conflict in a scope of the host, which keeps running" do
      pid = spawn_plain_process()
      ref = Process.monitor(pid)

      unregister_in_host_scope(pid, :syn_conflict_resolution)

      assert_received {RecordingEventHandler, :on_process_unregistered,
                       [@host_scope, @name, ^pid, :metadata, :syn_conflict_resolution]}

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 50
    end

    test "receives every other callback of the host's scopes" do
      for {callback, arguments} <- build_notifications(@host_scope) do
        apply(ConflictResolution, callback, arguments)

        assert_received {RecordingEventHandler, ^callback, ^arguments}
      end
    end

    test "hears nothing from the adapter's scopes", ctx do
      %{older: older, newer: newer} = ctx

      for {callback, arguments} <- build_notifications(@scope) do
        assert apply(ConflictResolution, callback, arguments) == :ok
      end

      capture_log(fn -> unregister_after_conflict(start_singleton()) end)

      assert ConflictResolution.resolve_registry_conflict(@scope, @name, newer, older) == pid_of(older)

      refute_received {RecordingEventHandler, _callback, [@scope | _arguments]}
    end
  end

  describe "an event handler installed before the adapter that leaves conflicts to syn" do
    setup do
      Process.register(self(), ListeningEventHandler)
      :ok = install_adapter_over(ListeningEventHandler)
    end

    test "gets syn's rule for the host's conflicts and hears of the loser syn's rule kills" do
      first = host_entry(:undefined, 1_000)
      last = host_entry(:undefined, 2_000)
      pid = spawn_plain_process()
      ref = Process.monitor(pid)

      assert ConflictResolution.resolve_registry_conflict(@host_scope, @name, first, last) == pid_of(last)

      unregister_in_host_scope(pid, :syn_conflict_resolution)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:syn_resolve_kill, @name, :metadata}}

      assert_received {ListeningEventHandler, :on_process_unregistered,
                       [@host_scope, @name, ^pid, :metadata, :syn_conflict_resolution]}
    end

    test "hears of the loser after the exit signal was sent to it, as with syn alone" do
      loser = spawn_relaying_process()

      unregister_in_host_scope(loser, :syn_conflict_resolution)

      assert_receive {:relayed, first_message}
      assert_receive {:relayed, second_message}
      exit_message = {:EXIT, self(), {:syn_resolve_kill, @name, :metadata}}
      assert [first_message, second_message] == [exit_message, {:unregistered, @name}]
    end

    test "gets only the callbacks it exports, told apart by arity" do
      for {callback, arguments} <- build_notifications(@host_scope) do
        apply(ConflictResolution, callback, arguments)
      end

      assert_received {ListeningEventHandler, :on_process_unregistered, [@host_scope, @name, _pid, :metadata, :normal]}

      assert_received {ListeningEventHandler, :on_registry_process_updated, [@host_scope, @name, _pid, :metadata, :normal]}
    end
  end

  describe "the adapter installed for several applications" do
    test "leaves the host's scopes to the event handler installed before the first application" do
      Process.register(self(), RecordingEventHandler)
      :ok = install_adapter_over(RecordingEventHandler)
      {:ok, [], _adapter_meta} = SynRegistry.child_spec(__MODULE__.OtherApp, [])
      arguments = [@host_scope, @name, self(), :metadata, :normal]

      # Were the adapter to take itself for the earlier handler, the call would
      # never return.
      notification = Task.async(ConflictResolution, :on_process_registered, arguments)

      assert Task.yield(notification, 1_000) || Task.shutdown(notification, :brutal_kill)
      assert Application.get_env(:syn, :event_handler) == ConflictResolution
      assert_received {RecordingEventHandler, :on_process_registered, ^arguments}
      refute_received {RecordingEventHandler, :on_process_registered, ^arguments}
    end
  end

  # Installs the adapter for `@scope` the way a Commanded application does at
  # boot, over the syn event handler configured at that moment.
  defp install_adapter_over(event_handler) do
    :ok = :syn.set_event_handler(event_handler)
    {:ok, [], _adapter_meta} = SynRegistry.child_spec(@scope, [])

    :ok
  end

  # Every callback but the two that decide a registry conflict, with the
  # arguments syn passes.
  defp build_notifications(scope) do
    pid = spawn_plain_process()

    [
      {:on_process_registered, [scope, @name, pid, :metadata, :normal]},
      {:on_registry_process_updated, [scope, @name, pid, :metadata, :normal]},
      {:on_registry_process_updated, [scope, @name, pid, :previous_metadata, :metadata, :normal]},
      {:on_process_unregistered, [scope, @name, pid, :metadata, :normal]},
      {:on_process_joined, [scope, @group, pid, :metadata, :normal]},
      {:on_group_process_updated, [scope, @group, pid, :metadata, :normal]},
      {:on_group_process_updated, [scope, @group, pid, :previous_metadata, :metadata, :normal]},
      {:on_process_left, [scope, @group, pid, :metadata, :normal]}
    ]
  end

  defp unregister_in_host_scope(pid, reason),
    do: ConflictResolution.on_process_unregistered(@host_scope, @name, pid, :metadata, reason)

  defp host_entry(metadata, time), do: {spawn_plain_process(), metadata, time}

  # A process that traps exits and passes every message it gets on to the test
  # process, exit signals included, in the order they arrive.
  defp spawn_relaying_process do
    test_process = self()
    pid = spawn(fn -> start_relaying(test_process) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    assert_receive {:relaying, ^pid}

    pid
  end

  defp start_relaying(test_process) do
    Process.flag(:trap_exit, true)
    send(test_process, {:relaying, self()})

    relay_messages(test_process)
  end

  defp relay_messages(test_process) do
    receive do
      message -> send(test_process, {:relayed, message})
    end

    relay_messages(test_process)
  end

  # A process that is not an OTP process and ignores every message.
  defp spawn_plain_process, do: spawn(Process, :sleep, [:infinity])

  defp unregister_after_conflict(pid), do: unregister_after_conflict(pid, build_registration_metadata(1_000, :singleton))

  defp unregister_after_conflict(pid, metadata) do
    ConflictResolution.on_process_unregistered(@scope, @name, pid, metadata, :syn_conflict_resolution)
  end

  defp unregister_after_conflict_and_settle(pid),
    do: unregister_after_conflict_and_settle(pid, build_registration_metadata(1_000, :singleton))

  # The stop runs in a process of its own, so give it time to report anything
  # it has to report while the log is still being captured.
  defp unregister_after_conflict_and_settle(pid, metadata) do
    unregister_after_conflict(pid, metadata)

    Process.sleep(100)
  end

  # A pid whose node is not running, standing in for a process on another node.
  defp build_pid_on_unreachable_node(node) do
    node_name = Atom.to_string(node)
    atom = <<100, byte_size(node_name)::16, node_name::binary>>

    :erlang.binary_to_term(<<131, 88, atom::binary, 1::32, 0::32, 1::32>>)
  end

  defp spawn_dead_process do
    {pid, ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    pid
  end

  defp registry_entry(started_at) do
    metadata = build_registration_metadata(started_at, :singleton)

    {start_singleton(), metadata, System.system_time()}
  end

  defp build_registration_metadata(started_at, kind), do: %{started_at: started_at, kind: kind}

  # The metadata of a registration made by a node that runs an older version
  # of the adapter.
  defp build_started_at_metadata(started_at), do: %{started_at: started_at}

  defp pid_of({pid, _metadata, _time}), do: pid

  # Unlinked, because a singleton that loses a conflict stops with a reason
  # that would take the test process down with it.
  defp start_singleton do
    {:ok, pid} = GenServer.start(Singleton, :state)
    on_exit(fn -> Process.exit(pid, :kill) end)

    pid
  end
end
