defmodule Commanded.Registration.SynRegistryClusterTest do
  use ExUnit.Case, async: false

  alias Commanded.Registration.SynRegistry.ClusterTestNode
  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.RecordingEventHandler

  @application __MODULE__.App
  @host_scope :cluster_test_host_scope
  @failover_delay_range {0, 0}
  @poll_timeout_ms 10_000

  # The peers run everything they need from `ClusterTestNode`. They start
  # unconnected, so each test decides when the two nodes meet. A test tagged
  # with `:earlier_event_handler` gets that syn event handler installed on
  # both peers before the adapter starts.
  setup ctx do
    {peer_a, node_a} = start_peer("a")
    {peer_b, node_b} = start_peer("b")
    :ok = install_earlier_event_handler(peer_a, ctx[:earlier_event_handler])
    :ok = install_earlier_event_handler(peer_b, ctx[:earlier_event_handler])
    registry_a = call(peer_a, :start_registry, [@application, @failover_delay_range])
    registry_b = call(peer_b, :start_registry, [@application, @failover_delay_range])
    name = {:handler, "cluster_singleton"}

    [
      peer_a: peer_a,
      node_a: node_a,
      peer_b: peer_b,
      node_b: node_b,
      registry_a: registry_a,
      registry_b: registry_b,
      name: name
    ]
  end

  test "the singleton started first keeps the name once the nodes meet", ctx do
    %{
      peer_a: peer_a,
      peer_b: peer_b,
      node_b: node_b,
      registry_a: registry_a,
      registry_b: registry_b,
      name: name
    } = ctx

    host_a = call(peer_a, :start_singleton, [registry_a, name])
    Process.sleep(20)
    host_b = call(peer_b, :start_singleton, [registry_b, name])
    {singleton_a, :singleton} = call(peer_a, :child, [host_a])
    {singleton_b, :singleton} = call(peer_b, :child, [host_b])
    :ok = call(peer_b, :watch_exit, [singleton_b])

    assert call(peer_a, :connect, [node_b])

    assert await_call(peer_a, :whereis, [registry_a, name], singleton_a) == singleton_a
    assert await_call(peer_b, :whereis, [registry_b, name], singleton_a) == singleton_a
    assert {_proxy, :proxy} = await_child(peer_b, host_b, :proxy)
    name_conflict = {:shutdown, :name_conflict}
    assert await_call(peer_b, :exit_reason, [singleton_b], name_conflict) == name_conflict
    assert call(peer_b, :alive?, [host_b])
  end

  test "the aggregate started first keeps the name once the nodes meet", ctx do
    %{
      peer_a: peer_a,
      peer_b: peer_b,
      node_b: node_b,
      registry_a: registry_a,
      registry_b: registry_b,
      name: name
    } = ctx

    aggregate_a = call(peer_a, :start_aggregate, [registry_a, name])
    Process.sleep(20)
    aggregate_b = call(peer_b, :start_aggregate, [registry_b, name])
    :ok = call(peer_b, :watch_exit, [aggregate_b])

    assert call(peer_a, :connect, [node_b])

    assert await_call(peer_a, :whereis, [registry_a, name], aggregate_a) == aggregate_a
    assert await_call(peer_b, :whereis, [registry_b, name], aggregate_a) == aggregate_a
    assert await_call(peer_b, :exit_reason, [aggregate_b], :normal) == :normal
    assert call(peer_a, :alive?, [aggregate_a])
  end

  test "the proxying node hosts the singleton after the hosting node goes away", ctx do
    %{
      peer_a: peer_a,
      peer_b: peer_b,
      node_b: node_b,
      registry_a: registry_a,
      registry_b: registry_b,
      name: name
    } = ctx

    assert call(peer_a, :connect, [node_b])
    host_a = call(peer_a, :start_singleton, [registry_a, name])
    {singleton_a, :singleton} = call(peer_a, :child, [host_a])
    await_call(peer_b, :whereis, [registry_b, name], singleton_a)
    host_b = call(peer_b, :start_singleton, [registry_b, name])
    assert {_proxy, :proxy} = call(peer_b, :child, [host_b])

    :ok = :peer.stop(peer_a)

    assert {singleton_b, :singleton} = await_child(peer_b, host_b, :singleton)
    assert await_call(peer_b, :whereis, [registry_b, name], singleton_b) == singleton_b
    assert call(peer_b, :alive?, [host_b])
  end

  test "a reaper whose host is killed leaves a name held on another node alone and ends normally",
       ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx

    holder = call(peer_b, :register_in_host_scope, [@host_scope, name, :metadata])
    assert call(peer_a, :connect, [node_b])
    :ok = :peer.call(peer_a, :syn, :add_node_to_scopes, [[@host_scope]])
    await_call(peer_a, :whereis_in_scope, [@host_scope, name], holder)

    reaper = call(peer_a, :start_reaper_of_killed_host, [@host_scope, name])

    assert await_call(peer_a, :exit_reason, [reaper], :normal) == :normal
    assert call(peer_b, :alive?, [holder])
  end

  test "a conflict in a scope of the host keeps the later registration and kills the other process, as syn does",
       ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx

    assert_syn_rule_decides_host_conflict(peer_a, peer_b, node_b, name)
  end

  # A node where syn's configuration names the adapter's handler before any
  # Commanded application starts has no earlier handler to pass scopes to.
  @tag earlier_event_handler: ConflictResolution
  test "a conflict in a scope of the host goes to syn's rule when the configuration names the adapter's handler",
       ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx

    assert_syn_rule_decides_host_conflict(peer_a, peer_b, node_b, name)
  end

  @tag earlier_event_handler: RecordingEventHandler
  test "a conflict in a scope of the host goes to the event handler installed before the adapter",
       ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx

    process_a = call(peer_a, :register_in_host_scope, [@host_scope, name, :metadata_a])
    Process.sleep(20)
    process_b = call(peer_b, :register_in_host_scope, [@host_scope, name, :metadata_b])
    :ok = call(peer_b, :watch_exit, [process_b])

    unregistration =
      {RecordingEventHandler, :on_process_unregistered,
       [@host_scope, name, process_b, :metadata_b, :syn_conflict_resolution]}

    assert call(peer_a, :connect, [node_b])

    # The recording handler keeps the registration made first.
    assert await_call(peer_b, :whereis_in_scope, [@host_scope, name], process_a) == process_a
    assert await_call(peer_a, :whereis_in_scope, [@host_scope, name], process_a) == process_a
    assert await_call(peer_b, :recorded?, [unregistration], true)
    # syn sends an exit signal, when it sends one, before it reports the loser,
    # so any signal is already on its way. Give it time to arrive.
    Process.sleep(100)
    assert call(peer_b, :exit_reason, [process_b]) == nil
    assert call(peer_b, :alive?, [process_b])
  end

  test "a process on another node that lost a conflict in a scope of the host is left to its own node",
       ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx
    assert call(peer_a, :connect, [node_b])
    process_b = call(peer_b, :register_in_host_scope, [@host_scope, name, :metadata_b])
    :ok = call(peer_b, :watch_exit, [process_b])

    :ok = call(peer_a, :report_conflict_loss, [@host_scope, name, process_b, :metadata_b])

    # An exit signal from the other node would arrive within this pause.
    Process.sleep(100)
    assert call(peer_b, :exit_reason, [process_b]) == nil
  end

  defp assert_syn_rule_decides_host_conflict(peer_a, peer_b, node_b, name) do
    process_a = call(peer_a, :register_in_host_scope, [@host_scope, name, :metadata_a])
    Process.sleep(20)
    process_b = call(peer_b, :register_in_host_scope, [@host_scope, name, :metadata_b])
    :ok = call(peer_a, :watch_exit, [process_a])
    kill_reason = {:syn_resolve_kill, name, :metadata_a}

    assert call(peer_a, :connect, [node_b])

    assert await_call(peer_a, :whereis_in_scope, [@host_scope, name], process_b) == process_b
    assert await_call(peer_b, :whereis_in_scope, [@host_scope, name], process_b) == process_b
    assert await_call(peer_a, :exit_reason, [process_a], kill_reason) == kill_reason
    assert call(peer_b, :alive?, [process_b])
  end

  defp install_earlier_event_handler(_peer, nil), do: :ok

  defp install_earlier_event_handler(peer, event_handler) do
    :ok = call(peer, :start_recording, [])

    call(peer, :install_event_handler, [event_handler])
  end

  defp start_peer(suffix) do
    name = :"syn_registry_#{suffix}_#{System.unique_integer([:positive])}"

    peer_options = %{
      name: name,
      host: ~c"127.0.0.1",
      longnames: true,
      connection: :standard_io,
      args: peer_args()
    }

    {:ok, peer, node} = :peer.start_link(peer_options)
    # Every restart and every lost connection these tests provoke is reported
    # by the peer's supervisors on the test node's console. Lower the level to
    # read them.
    :ok = :peer.call(peer, :logger, :set_primary_config, [:level, :critical])

    {peer, node}
  end

  # The peers load this project and its dependencies from the code paths of
  # the test node, and they stay out of each other's way until a test connects
  # them.
  defp peer_args do
    code_paths = Enum.flat_map(:code.get_path(), fn directory -> [~c"-pa", directory] end)

    code_paths ++ [~c"-connect_all", ~c"false"]
  end

  defp call(peer, function, args), do: :peer.call(peer, ClusterTestNode, function, args)

  defp await_call(peer, function, args, expected),
    do: await_call(peer, function, args, expected, poll_deadline())

  defp await_call(peer, function, args, expected, deadline) do
    result = call(peer, function, args)

    cond do
      result == expected ->
        result

      System.monotonic_time(:millisecond) > deadline ->
        flunk("#{function} returned #{inspect(result)}, expected #{inspect(expected)}")

      true ->
        Process.sleep(20)
        await_call(peer, function, args, expected, deadline)
    end
  end

  defp await_child(peer, host, expected_kind),
    do: await_child(peer, host, expected_kind, poll_deadline())

  defp await_child(peer, host, expected_kind, deadline) do
    child = call(peer, :child, [host])

    cond do
      match?({_pid, ^expected_kind}, child) ->
        child

      System.monotonic_time(:millisecond) > deadline ->
        flunk("the host runs #{inspect(child)}, expected a #{expected_kind}")

      true ->
        Process.sleep(20)
        await_child(peer, host, expected_kind, deadline)
    end
  end

  defp poll_deadline, do: System.monotonic_time(:millisecond) + @poll_timeout_ms
end
