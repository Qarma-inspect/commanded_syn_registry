defmodule Commanded.Registration.SynRegistry.ConflictResolutionTest do
  use ExUnit.Case, async: false

  import Commanded.Registration.SynRegistry.AbsentProcess
  import ExUnit.CaptureLog

  alias Commanded.Registration.SynRegistry.ConflictResolution

  defmodule Singleton do
    use GenServer

    def init(state), do: {:ok, state}
  end

  # The scope of a Commanded application, which the adapter creates.
  @scope __MODULE__.App
  @name {:handler, "conflicted"}

  setup do
    older = build_registry_entry(1_000)
    newer = build_registry_entry(2_000)
    [older: older, newer: newer]
  end

  describe "resolve_registry_conflict/4" do
    test "keeps the process that was started first whichever side of the conflict it arrives from", ctx do
      %{older: older, newer: newer} = ctx

      assert ConflictResolution.resolve_registry_conflict(@scope, @name, newer, older) == extract_pid(older)
      assert ConflictResolution.resolve_registry_conflict(@scope, @name, older, newer) == extract_pid(older)
    end

    test "keeps the same process for two processes started at the same time", ctx do
      %{older: older} = ctx
      other = build_registry_entry(1_000)
      older_pid = extract_pid(older)
      other_pid = extract_pid(other)

      kept = ConflictResolution.resolve_registry_conflict(@scope, @name, older, other)

      assert kept == max(older_pid, other_pid)
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
      metadata = build_registration_metadata(1_000, :singleton)
      refute Process.alive?(dead)

      log = capture_log(fn -> unregister_after_conflict_and_settle(dead, metadata) end)

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
      node_down_reason = {:syn_remote_scope_node_down, @scope, node()}

      assert ConflictResolution.on_process_unregistered(@scope, @name, pid, metadata, :normal) == :ok

      assert ConflictResolution.on_process_unregistered(@scope, @name, pid, metadata, node_down_reason) == :ok

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 50
      assert Process.alive?(pid)
    end
  end

  defp unregister_after_conflict(pid), do: unregister_after_conflict(pid, build_registration_metadata(1_000, :singleton))

  defp unregister_after_conflict(pid, metadata) do
    ConflictResolution.on_process_unregistered(@scope, @name, pid, metadata, :syn_conflict_resolution)
  end

  # The stop runs in a process of its own, so give it time to report anything
  # it has to report while the log is still being captured.
  defp unregister_after_conflict_and_settle(pid, metadata) do
    unregister_after_conflict(pid, metadata)

    Process.sleep(100)
  end

  defp build_registry_entry(started_at) do
    singleton = start_singleton()
    metadata = build_registration_metadata(started_at, :singleton)

    {singleton, metadata, System.system_time()}
  end

  defp build_registration_metadata(started_at, kind), do: %{started_at: started_at, kind: kind}

  # Registration metadata that has no `:kind` key.
  defp build_started_at_metadata(started_at), do: %{started_at: started_at}

  defp extract_pid({pid, _metadata, _time}), do: pid

  # Unlinked, because a singleton that loses a conflict stops with a reason
  # that would take the test process down with it.
  defp start_singleton do
    {:ok, pid} = GenServer.start(Singleton, :state)
    on_exit(fn -> Process.exit(pid, :kill) end)

    pid
  end
end
