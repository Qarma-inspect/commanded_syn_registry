defmodule Commanded.Registration.SynRegistryClusterTest do
  use ExUnit.Case, async: false

  alias Commanded.Registration.SynRegistry.ClusterTestNode
  alias Commanded.Registration.SynRegistry.ClusterTestQuickRejectingSingleton
  alias Commanded.Registration.SynRegistry.ClusterTestRejectingSingleton
  alias Commanded.Registration.SynRegistry.ClusterTestSingleton
  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.Polling
  alias Commanded.Registration.SynRegistry.RecordingEventHandler

  @application __MODULE__.App
  @host_scope :cluster_test_host_scope
  @failover_delay_range {0, 0}
  @default_failover_delay_range {200, 1_000}
  @poll_timeout_ms 10_000
  # Elixir's `Supervisor` default restart intensity, written out for the test
  # that runs the parents out of it.
  @parent_options [strategy: :one_for_one, max_restarts: 3, max_seconds: 5]

  # The peers run everything they need from `ClusterTestNode`. They start
  # unconnected, so each test decides when the two nodes meet. A test tagged
  # with `:earlier_event_handler` gets that syn event handler installed on
  # both peers before the adapter starts, and a test tagged with
  # `:failover_delay_range` gets registries with that range.
  setup ctx do
    failover_delay_range = Map.get(ctx, :failover_delay_range, @failover_delay_range)
    {peer_a, node_a} = start_peer("a")
    {peer_b, node_b} = start_peer("b")
    :ok = install_earlier_event_handler(peer_a, ctx[:earlier_event_handler])
    :ok = install_earlier_event_handler(peer_b, ctx[:earlier_event_handler])
    registry_a = call(peer_a, :start_registry, [@application, failover_delay_range])
    registry_b = call(peer_b, :start_registry, [@application, failover_delay_range])
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
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, registry_a: registry_a, registry_b: registry_b, name: name} = ctx

    host_a = call(peer_a, :start_singleton, [registry_a, name])
    Process.sleep(20)
    host_b = call(peer_b, :start_singleton, [registry_b, name])
    {singleton_a, :singleton} = call(peer_a, :fetch_child, [host_a])
    {singleton_b, :singleton} = call(peer_b, :fetch_child, [host_b])
    :ok = call(peer_b, :watch_exit, [singleton_b])

    assert call(peer_a, :connect, [node_b])

    assert await_call(peer_a, :whereis, [registry_a, name], singleton_a) == singleton_a
    assert await_call(peer_b, :whereis, [registry_b, name], singleton_a) == singleton_a
    assert {_proxy, :proxy} = await_child(peer_b, host_b, :proxy)
    name_conflict = {:shutdown, :name_conflict}
    assert await_call(peer_b, :fetch_exit_reason, [singleton_b], name_conflict) == name_conflict
    assert call(peer_b, :alive?, [host_b])
  end

  test "the aggregate started first keeps the name once the nodes meet", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, registry_a: registry_a, registry_b: registry_b, name: name} = ctx

    aggregate_a = call(peer_a, :start_aggregate, [registry_a, name])
    Process.sleep(20)
    aggregate_b = call(peer_b, :start_aggregate, [registry_b, name])
    :ok = call(peer_b, :watch_exit, [aggregate_b])

    assert call(peer_a, :connect, [node_b])

    assert await_call(peer_a, :whereis, [registry_a, name], aggregate_a) == aggregate_a
    assert await_call(peer_b, :whereis, [registry_b, name], aggregate_a) == aggregate_a
    assert await_call(peer_b, :fetch_exit_reason, [aggregate_b], :normal) == :normal
    assert call(peer_a, :alive?, [aggregate_a])
  end

  test "the proxying node hosts the singleton after the hosting node goes away", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, registry_a: registry_a, registry_b: registry_b, name: name} = ctx

    assert call(peer_a, :connect, [node_b])
    host_a = call(peer_a, :start_singleton, [registry_a, name])
    {singleton_a, :singleton} = call(peer_a, :fetch_child, [host_a])
    await_call(peer_b, :whereis, [registry_b, name], singleton_a)
    host_b = call(peer_b, :start_singleton, [registry_b, name])
    assert {_proxy, :proxy} = call(peer_b, :fetch_child, [host_b])

    :ok = :peer.stop(peer_a)

    assert {singleton_b, :singleton} = await_child(peer_b, host_b, :singleton)
    assert await_call(peer_b, :whereis, [registry_b, name], singleton_b) == singleton_b
    assert call(peer_b, :alive?, [host_b])
  end

  test "node loss hands the name to the other node without reaching its parent", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, registry_a: registry_a, registry_b: registry_b, name: name} = ctx
    assert call(peer_a, :connect, [node_b])
    host_a = call(peer_a, :start_singleton, [registry_a, name])
    {singleton_a, :singleton} = call(peer_a, :fetch_child, [host_a])
    await_call(peer_b, :whereis, [registry_b, name], singleton_a)
    {parent_b, host_b, proxy_b} = start_proxying_parent(peer_b, registry_b, name, :temporary)

    :ok = :peer.stop(peer_a)

    assert await_call(peer_b, :fetch_exit_reason, [proxy_b], :noconnection) == :noconnection
    assert {singleton_b, :singleton} = await_child(peer_b, host_b, :singleton)
    assert await_call(peer_b, :whereis, [registry_b, name], singleton_b) == singleton_b
    assert call(peer_b, :fetch_child, [parent_b]) == {host_b, :host}
  end

  test "a graceful stop of the holder's node hands the name to the other node without reaching its parent", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, registry_a: registry_a, registry_b: registry_b, name: name} = ctx
    assert call(peer_a, :connect, [node_b])
    supervisor_a = call(peer_a, :start_singleton_application, [registry_a, name])
    {host_a, :host} = call(peer_a, :fetch_child, [supervisor_a])
    {singleton_a, :singleton} = call(peer_a, :fetch_child, [host_a])
    await_call(peer_b, :whereis, [registry_b, name], singleton_a)
    {parent_b, host_b, proxy_b} = start_proxying_parent(peer_b, registry_b, name, :temporary)

    :ok = :peer.cast(peer_a, :init, :stop, [])

    assert await_call(peer_b, :fetch_exit_reason, [proxy_b], :shutdown) == :shutdown
    assert {singleton_b, :singleton} = await_child(peer_b, host_b, :singleton)
    assert await_call(peer_b, :whereis, [registry_b, name], singleton_b) == singleton_b
    assert call(peer_b, :fetch_child, [parent_b]) == {host_b, :host}
  end

  test "permanent parents on two nodes count the same stops and stop in the same round", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, registry_a: registry_a, registry_b: registry_b, name: name} = ctx
    assert call(peer_a, :connect, [node_b])
    :ok = :peer.call(peer_a, ClusterTestRejectingSingleton, :hold_first_stop, [])
    parent_a = start_parent(peer_a, registry_a, name, ClusterTestRejectingSingleton, :permanent)
    :ok = call(peer_a, :watch_exit, [parent_a])
    {host_a, :host} = call(peer_a, :fetch_child, [parent_a])
    {singleton_a, :singleton} = call(peer_a, :fetch_child, [host_a])
    await_call(peer_b, :whereis, [registry_b, name], singleton_a)
    parent_b = start_parent(peer_b, registry_b, name, ClusterTestRejectingSingleton, :permanent)
    :ok = call(peer_b, :watch_exit, [parent_b])

    :ok = :peer.call(peer_a, ClusterTestRejectingSingleton, :reject, [singleton_a])

    assert await_call(peer_a, :fetch_exit_reason, [parent_a], :shutdown) == :shutdown
    assert await_call(peer_b, :fetch_exit_reason, [parent_b], :shutdown) == :shutdown
    stops_a = :peer.call(peer_a, ClusterTestRejectingSingleton, :count_stops, [])
    stops_b = :peer.call(peer_b, ClusterTestRejectingSingleton, :count_stops, [])
    assert stops_a + stops_b == 4
  end

  @tag failover_delay_range: @default_failover_delay_range
  test "permanent parents on two nodes count the same stops of a singleton that stops sooner than the failover delay", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, registry_a: registry_a, registry_b: registry_b, name: name} = ctx
    assert call(peer_a, :connect, [node_b])
    :ok = :peer.call(peer_a, ClusterTestQuickRejectingSingleton, :hold_first_stop, [])
    parent_a = start_parent(peer_a, registry_a, name, ClusterTestQuickRejectingSingleton, :permanent)
    :ok = call(peer_a, :watch_exit, [parent_a])
    {host_a, :host} = call(peer_a, :fetch_child, [parent_a])
    {singleton_a, :singleton} = call(peer_a, :fetch_child, [host_a])
    await_call(peer_b, :whereis, [registry_b, name], singleton_a)
    parent_b = start_parent(peer_b, registry_b, name, ClusterTestQuickRejectingSingleton, :permanent)
    :ok = call(peer_b, :watch_exit, [parent_b])

    :ok = :peer.call(peer_a, ClusterTestQuickRejectingSingleton, :reject, [singleton_a])

    assert await_call(peer_a, :fetch_exit_reason, [parent_a], :shutdown) == :shutdown
    assert await_call(peer_b, :fetch_exit_reason, [parent_b], :shutdown) == :shutdown
    stops_a = :peer.call(peer_a, ClusterTestQuickRejectingSingleton, :count_stops, [])
    stops_b = :peer.call(peer_b, ClusterTestQuickRejectingSingleton, :count_stops, [])
    assert stops_a + stops_b == 4
  end

  # The registry process on A is held from before the first stop until 50 ms
  # after it, so B's parent starts the host again while B's table still
  # names the singleton that stopped.
  @tag failover_delay_range: @default_failover_delay_range
  test "permanent parents on two nodes count the same stops when the holder's node reports the first stop late", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, registry_a: registry_a, registry_b: registry_b, name: name} = ctx
    assert call(peer_a, :connect, [node_b])
    :ok = :peer.call(peer_a, ClusterTestQuickRejectingSingleton, :hold_first_stop, [])
    parent_a = start_parent(peer_a, registry_a, name, ClusterTestQuickRejectingSingleton, :permanent)
    :ok = call(peer_a, :watch_exit, [parent_a])
    {host_a, :host} = call(peer_a, :fetch_child, [parent_a])
    {singleton_a, :singleton} = call(peer_a, :fetch_child, [host_a])
    await_call(peer_b, :whereis, [registry_b, name], singleton_a)
    parent_b = start_parent(peer_b, registry_b, name, ClusterTestQuickRejectingSingleton, :permanent)
    :ok = call(peer_b, :watch_exit, [parent_b])
    {host_b, :host} = call(peer_b, :fetch_child, [parent_b])
    {_proxy_b, :proxy} = call(peer_b, :fetch_child, [host_b])

    :ok = call(peer_a, :hold_registry_past_exit, [@application, singleton_a, 50])
    :ok = :peer.call(peer_a, ClusterTestQuickRejectingSingleton, :reject, [singleton_a])

    assert await_call(peer_a, :fetch_exit_reason, [parent_a], :shutdown) == :shutdown
    assert await_call(peer_b, :fetch_exit_reason, [parent_b], :shutdown) == :shutdown
    stops_a = :peer.call(peer_a, ClusterTestQuickRejectingSingleton, :count_stops, [])
    stops_b = :peer.call(peer_b, ClusterTestQuickRejectingSingleton, :count_stops, [])
    assert stops_a + stops_b == 4
  end

  describe "the holder's own exits" do
    @describetag restart: :temporary

    # Each node runs a parent with a host for the same name, under the restart
    # type the test is tagged with: the host on A holds the name, and the host
    # on B proxies it.
    setup ctx do
      %{
        peer_a: peer_a,
        peer_b: peer_b,
        node_b: node_b,
        registry_a: registry_a,
        registry_b: registry_b,
        name: name,
        restart: restart
      } = ctx

      true = call(peer_a, :connect, [node_b])
      {parent_a, host_a, holder} = start_holding_parent(peer_a, registry_a, name, restart)
      :ok = call(peer_a, :watch_exit, [host_a])
      await_call(peer_b, :whereis, [registry_b, name], holder)
      {parent_b, host_b, _proxy_b} = start_proxying_parent(peer_b, registry_b, name, restart)
      :ok = call(peer_b, :watch_exit, [host_b])

      [parent_a: parent_a, host_a: host_a, holder: holder, parent_b: parent_b, host_b: host_b]
    end

    test "a stop the holder asks for on one node ends the proxying host on the other node with the same reason", ctx do
      %{
        peer_a: peer_a,
        peer_b: peer_b,
        registry_a: registry_a,
        registry_b: registry_b,
        name: name,
        parent_a: parent_a,
        host_a: host_a,
        holder: holder,
        parent_b: parent_b,
        host_b: host_b
      } = ctx

      reason = {:rejected, "event"}

      :ok = :peer.call(peer_a, GenServer, :stop, [holder, reason])

      assert await_call(peer_b, :fetch_exit_reason, [host_b], reason) == reason
      assert await_call(peer_a, :fetch_exit_reason, [host_a], reason) == reason
      assert :peer.call(peer_a, Supervisor, :which_children, [parent_a]) == []
      assert :peer.call(peer_b, Supervisor, :which_children, [parent_b]) == []
      assert await_call(peer_a, :whereis, [registry_a, name], :undefined) == :undefined
      assert await_call(peer_b, :whereis, [registry_b, name], :undefined) == :undefined
    end

    @tag restart: :transient
    test "a handler stopped with {:shutdown, reason} under transient parents is not restarted on either node", ctx do
      %{
        peer_a: peer_a,
        peer_b: peer_b,
        registry_a: registry_a,
        registry_b: registry_b,
        name: name,
        parent_a: parent_a,
        host_a: host_a,
        holder: holder,
        parent_b: parent_b,
        host_b: host_b
      } = ctx

      reason = {:shutdown, :rejected}

      :ok = :peer.call(peer_a, GenServer, :stop, [holder, reason])

      assert await_call(peer_b, :fetch_exit_reason, [host_b], reason) == reason
      assert await_call(peer_a, :fetch_exit_reason, [host_a], reason) == reason
      assert [{_id, :undefined, _type, _modules}] = :peer.call(peer_a, Supervisor, :which_children, [parent_a])
      assert [{_id, :undefined, _type, _modules}] = :peer.call(peer_b, Supervisor, :which_children, [parent_b])
      assert await_call(peer_a, :whereis, [registry_a, name], :undefined) == :undefined
      assert await_call(peer_b, :whereis, [registry_b, name], :undefined) == :undefined
    end

    test "a crash of the holder ends the proxying host on the other node with the crash reason", ctx do
      %{peer_a: peer_a, peer_b: peer_b, holder: holder, parent_b: parent_b, host_b: host_b} = ctx

      true = :peer.call(peer_a, Process, :exit, [holder, :crashed])

      assert await_call(peer_b, :fetch_exit_reason, [host_b], :crashed) == :crashed
      assert :peer.call(peer_b, Supervisor, :which_children, [parent_b]) == []
    end
  end

  test "a reaper whose host is killed leaves a name held on another node alone and ends normally", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx

    holder = call(peer_b, :register_in_host_scope, [@host_scope, name, :metadata])
    assert call(peer_a, :connect, [node_b])
    :ok = :peer.call(peer_a, :syn, :add_node_to_scopes, [[@host_scope]])
    await_call(peer_a, :whereis_in_scope, [@host_scope, name], holder)

    reaper = call(peer_a, :start_reaper_of_killed_host, [@host_scope, name])

    assert await_call(peer_a, :fetch_exit_reason, [reaper], :normal) == :normal
    assert call(peer_b, :alive?, [holder])
  end

  test "a conflict in a scope of the host keeps the later registration and kills the other process, as syn does", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx

    assert_syn_rule_decides_host_conflict(peer_a, peer_b, node_b, name)
  end

  # A node where syn's configuration names the adapter's handler before any
  # Commanded application starts has no earlier handler to pass scopes to.
  @tag earlier_event_handler: ConflictResolution
  test "a conflict in a scope of the host goes to syn's rule when the configuration names the adapter's handler", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx

    assert_syn_rule_decides_host_conflict(peer_a, peer_b, node_b, name)
  end

  @tag earlier_event_handler: RecordingEventHandler
  test "a conflict in a scope of the host goes to the event handler installed before the adapter", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx

    process_a = call(peer_a, :register_in_host_scope, [@host_scope, name, :metadata_a])
    Process.sleep(20)
    process_b = call(peer_b, :register_in_host_scope, [@host_scope, name, :metadata_b])
    :ok = call(peer_b, :watch_exit, [process_b])

    unregistration =
      {RecordingEventHandler, :on_process_unregistered, [@host_scope, name, process_b, :metadata_b, :syn_conflict_resolution]}

    assert call(peer_a, :connect, [node_b])

    # The recording handler keeps the registration made first.
    assert await_call(peer_b, :whereis_in_scope, [@host_scope, name], process_a) == process_a
    assert await_call(peer_a, :whereis_in_scope, [@host_scope, name], process_a) == process_a
    assert await_call(peer_b, :recorded?, [unregistration], true)
    # syn sends an exit signal, when it sends one, before it reports the loser,
    # so any signal is already on its way. Give it time to arrive.
    Process.sleep(100)
    assert call(peer_b, :fetch_exit_reason, [process_b]) == nil
    assert call(peer_b, :alive?, [process_b])
  end

  test "a process on another node that lost a conflict in a scope of the host is left to its own node", ctx do
    %{peer_a: peer_a, peer_b: peer_b, node_b: node_b, name: name} = ctx
    assert call(peer_a, :connect, [node_b])
    process_b = call(peer_b, :register_in_host_scope, [@host_scope, name, :metadata_b])
    :ok = call(peer_b, :watch_exit, [process_b])

    :ok = call(peer_a, :report_conflict_loss, [@host_scope, name, process_b, :metadata_b])

    # An exit signal from the other node would arrive within this pause.
    Process.sleep(100)
    assert call(peer_b, :fetch_exit_reason, [process_b]) == nil
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
    assert await_call(peer_a, :fetch_exit_reason, [process_a], kill_reason) == kill_reason
    assert call(peer_b, :alive?, [process_b])
  end

  defp start_parent(peer, registry, name, singleton_module, restart) do
    call(peer, :start_supervised_singleton, [registry, name, singleton_module, [restart: restart], @parent_options])
  end

  # Starts a parent on the peer whose host holds the name, and returns the
  # parent, the host and the singleton.
  defp start_holding_parent(peer, registry, name, restart) do
    parent = start_parent(peer, registry, name, ClusterTestSingleton, restart)
    {host, :host} = call(peer, :fetch_child, [parent])
    {singleton, :singleton} = call(peer, :fetch_child, [host])

    {parent, host, singleton}
  end

  # Starts a parent on the peer whose host proxies the singleton held on the
  # other node, and watches the proxy's exit.
  defp start_proxying_parent(peer, registry, name, restart) do
    parent = start_parent(peer, registry, name, ClusterTestSingleton, restart)
    {host, :host} = call(peer, :fetch_child, [parent])
    {proxy, :proxy} = call(peer, :fetch_child, [host])
    :ok = call(peer, :watch_exit, [proxy])

    {parent, host, proxy}
  end

  defp install_earlier_event_handler(_peer, nil), do: :ok

  defp install_earlier_event_handler(peer, event_handler) do
    :ok = call(peer, :start_recording, [])

    call(peer, :install_event_handler, [event_handler])
  end

  defp start_peer(suffix) do
    name = :"syn_registry_#{suffix}_#{System.unique_integer([:positive])}"

    peer_options = %{name: name, host: ~c"127.0.0.1", longnames: true, connection: :standard_io, args: build_peer_args()}

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
  defp build_peer_args do
    code_paths = Enum.flat_map(:code.get_path(), fn directory -> [~c"-pa", directory] end)

    code_paths ++ [~c"-connect_all", ~c"false"]
  end

  defp call(peer, function, args), do: :peer.call(peer, ClusterTestNode, function, args)

  defp await_call(peer, function, args, expected) do
    Polling.await(@poll_timeout_ms, fn ->
      result = call(peer, function, args)

      if result == expected,
        do: {:ok, result},
        else: {:error, "#{function} returned #{inspect(result)}, expected #{inspect(expected)}"}
    end)
  end

  defp await_child(peer, host, expected_kind) do
    Polling.await(@poll_timeout_ms, fn ->
      child = call(peer, :fetch_child, [host])

      if match?({_pid, ^expected_kind}, child),
        do: {:ok, child},
        else: {:error, "the host runs #{inspect(child)}, expected a #{expected_kind}"}
    end)
  end
end
