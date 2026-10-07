defmodule Commanded.Registration.SynRegistry.SingletonProxyTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  import ExUnit.CaptureLog

  alias Commanded.Registration.SynRegistry.SingletonProxy

  @name {:handler, "singleton_proxy_test"}

  setup do
    monitored = spawn_monitored_process()
    [monitored: monitored]
  end

  describe "start_link/3" do
    test "starts a live proxy that is not the monitored process", ctx do
      %{monitored: monitored} = ctx

      assert {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      assert proxy != monitored
      assert Process.alive?(proxy)
    end

    test "monitors the process and keeps the name and the delay range", ctx do
      %{monitored: monitored} = ctx

      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {10, 20})

      assert %SingletonProxy{pid: ^monitored, name: @name, failover_delay_range: {10, 20}} = :sys.get_state(proxy)

      assert {:monitored_by, [^proxy]} = Process.info(monitored, :monitored_by)
    end

    test "links the caller to the proxy but not to the monitored process", ctx do
      %{monitored: monitored} = ctx

      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      {:links, links} = Process.info(self(), :links)
      assert proxy in links
      refute monitored in links
    end
  end

  describe "a proxy while the monitored process is running" do
    test "ignores messages it does not know", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      send(proxy, :unexpected_message)
      send(proxy, {:DOWN, make_ref(), :process, monitored, :unrelated})
      %SingletonProxy{monitor_ref: monitor_ref} = :sys.get_state(proxy)
      send(proxy, {:DOWN, monitor_ref, :process, spawn_dead_process(), :unrelated})

      refute_receive {:EXIT, ^proxy, _reason}, 50
      assert Process.alive?(proxy)
    end

    test "leaves the monitored process alive when the proxy is shut down", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      Process.exit(proxy, :shutdown)

      assert_receive {:EXIT, ^proxy, :shutdown}
      assert Process.alive?(monitored)
    end
  end

  describe "a proxy after the monitored process goes down" do
    test "exits with the exit reason of the monitored process", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      Process.exit(monitored, :kill)

      assert_receive {:EXIT, ^proxy, :killed}
    end

    test "exits normally when the monitored process stops normally", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      send(monitored, :stop)

      assert_receive {:EXIT, ^proxy, :normal}
    end

    test "exits without a crash report when the node hosting the process is lost", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      log = capture_log(fn -> stop_monitored_and_await_proxy_exit(monitored, proxy, :noconnection) end)

      refute log =~ "terminating"
      refute log =~ "[error]"
    end

    test "logs the name, the node, the reason and the delay", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {7, 7})

      log = capture_log(fn -> stop_monitored_and_await_proxy_exit(monitored, proxy, :boom) end)

      expected =
        "[info] SingletonProxy: singleton down, stopping the proxy: name=#{inspect(@name)} node=#{inspect(node())} reason=:boom delay_ms=7"

      assert log =~ expected
    end

    test "logs the node of a monitored process on another node and exits with :noconnection" do
      Process.flag(:trap_exit, true)
      remote = build_pid_on_unreachable_node(:"unreachable@127.0.0.1")

      log = capture_log(fn -> start_proxy_and_await_exit(remote, :noconnection) end)

      assert log =~ "node=:\"unreachable@127.0.0.1\" reason=:noconnection"
    end

    test "waits for a delay within the configured range before exiting", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {150, 200})

      Process.exit(monitored, :kill)

      refute_receive {:EXIT, ^proxy, _reason}, 100
      assert_receive {:EXIT, ^proxy, :killed}, 1_000
    end

    test "can be shut down while it waits", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {5_000, 5_000})
      Process.exit(monitored, :kill)
      refute_receive {:EXIT, ^proxy, _reason}, 50

      Process.exit(proxy, :shutdown)

      assert_receive {:EXIT, ^proxy, :shutdown}, 100
    end
  end

  describe "a proxy for a process that is already dead" do
    test "waits for the failover delay and then exits with :noproc" do
      Process.flag(:trap_exit, true)
      dead = spawn_dead_process()

      {:ok, proxy} = SingletonProxy.start_link(dead, @name, {100, 100})

      refute_receive {:EXIT, ^proxy, _reason}, 50
      assert_receive {:EXIT, ^proxy, :noproc}, 1_000
    end
  end

  describe "draw_failover_delay/1" do
    test "draws delays within the bounds and reaches both of them" do
      draws = Stream.repeatedly(fn -> SingletonProxy.draw_failover_delay({150, 200}) end)
      delays = Enum.take(draws, 1_000)

      assert Enum.all?(delays, &(&1 in 150..200))
      assert Enum.min(delays) == 150
      assert Enum.max(delays) == 200
    end

    test "draws the same delay when both bounds are equal" do
      assert SingletonProxy.draw_failover_delay({0, 0}) == 0
      assert SingletonProxy.draw_failover_delay({150, 150}) == 150
    end
  end

  describe "validate_failover_delay_range!/1" do
    test "returns a range of non-negative integers whose minimum does not exceed its maximum" do
      assert SingletonProxy.validate_failover_delay_range!({0, 0}) == {0, 0}
      assert SingletonProxy.validate_failover_delay_range!({200, 1_000}) == {200, 1_000}
    end

    test "raises for a range whose minimum exceeds its maximum" do
      assert_raise ArgumentError, ~r/min <= max/, fn -> SingletonProxy.validate_failover_delay_range!({500, 100}) end
    end

    test "raises for a negative minimum" do
      assert_raise ArgumentError, fn -> SingletonProxy.validate_failover_delay_range!({-1, 100}) end
    end

    test "raises for bounds that are not integers" do
      assert_raise ArgumentError, fn -> SingletonProxy.validate_failover_delay_range!({1.0, 2}) end

      assert_raise ArgumentError, fn -> SingletonProxy.validate_failover_delay_range!({1, 2.0}) end
    end

    test "raises for a value that is not a pair" do
      assert_raise ArgumentError, fn -> SingletonProxy.validate_failover_delay_range!({1, 2, 3}) end

      assert_raise ArgumentError, fn -> SingletonProxy.validate_failover_delay_range!(200) end
    end

    test "shows the rejected value in the message" do
      message = "got: {:bad, :range}"

      assert_raise ArgumentError, ~r/#{message}/, fn -> SingletonProxy.validate_failover_delay_range!({:bad, :range}) end
    end
  end

  # A process that runs until told to stop, owned by no test, so that the
  # test process is not linked to it.
  defp spawn_monitored_process do
    spawn(fn ->
      receive do
        :stop -> :ok
      end
    end)
  end

  # The pause gives the proxy's exit time to be logged, if it were.
  defp stop_monitored_and_await_proxy_exit(monitored, proxy, reason) do
    Process.exit(monitored, reason)
    assert_receive {:EXIT, ^proxy, ^reason}
    Process.sleep(50)
  end

  defp start_proxy_and_await_exit(monitored, reason) do
    {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})
    assert_receive {:EXIT, ^proxy, ^reason}
    Process.sleep(50)
  end

  # A pid whose node is not running: monitoring it reports `:noconnection`
  # straight away, the way a lost connection to the hosting node does.
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
end
