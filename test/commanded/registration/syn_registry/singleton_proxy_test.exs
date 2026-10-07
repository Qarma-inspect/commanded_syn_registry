defmodule Commanded.Registration.SynRegistry.SingletonProxyTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  import Commanded.Registration.SynRegistry.AbsentProcess
  import ExUnit.CaptureLog

  alias Commanded.Registration.SynRegistry.Polling
  alias Commanded.Registration.SynRegistry.SingletonProxy

  # A registry whose names are pids. Each name resolves to the pid it
  # carries, alive or not, as syn on one node resolves a name to a holder on
  # another node until that node reports the holder's exit.
  defmodule StaleRegistry do
    def whereis_name(pid), do: pid
  end

  # A registry whose name resolves to a holder that is gone until
  # `replaced_at_ms` on the monotonic clock, and from then on to a
  # replacement that is gone as well.
  defmodule GoneReplacementRegistry do
    def whereis_name({gone_holder, gone_replacement, replaced_at_ms}) do
      if System.monotonic_time(:millisecond) >= replaced_at_ms,
        do: gone_replacement,
        else: gone_holder
    end
  end

  @scope __MODULE__.App
  @name {:via, :syn, {@scope, {:handler, "singleton_proxy_test"}}}

  setup do
    :ok = :syn.add_node_to_scopes([@scope])

    # A process that runs until told to stop, owned by no test, so that the
    # test process is not linked to it.
    monitored =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

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
      unrelated_pid = spawn_dead_process()
      send(proxy, {:DOWN, monitor_ref, :process, unrelated_pid, :unrelated})

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

    test "logs the name, the node, the reason and the delay when it fails over", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {7, 7})

      log = capture_log(fn -> stop_monitored_and_await_proxy_exit(monitored, proxy, :shutdown) end)

      expected =
        "[info] SingletonProxy: singleton down, stopping the proxy: name=#{inspect(@name)} node=#{inspect(node())} reason=:shutdown delay_ms=7"

      assert log =~ expected
    end

    test "logs the name, the node and the reason when it passes the holder's own exit on", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {7, 7})

      log = capture_log(fn -> stop_monitored_and_await_proxy_exit(monitored, proxy, :boom) end)

      expected =
        "[info] SingletonProxy: singleton exited on its own, passing the exit on: name=#{inspect(@name)} node=#{inspect(node())} reason=:boom"

      assert log =~ expected
      refute log =~ "delay_ms"
    end

    test "passes a crash of the monitored process on without the failover delay", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {5_000, 5_000})

      Process.exit(monitored, :crashed)

      assert_receive {:EXIT, ^proxy, :crashed}, 100
    end

    test "passes a crash of the monitored process on without a crash report", ctx do
      %{monitored: monitored} = ctx
      Process.flag(:trap_exit, true)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      log = capture_log(fn -> stop_monitored_and_await_proxy_exit(monitored, proxy, :crashed) end)

      refute log =~ "terminating"
      refute log =~ "[error]"
    end

    test "passes an exit with reason :kill on unchanged" do
      Process.flag(:trap_exit, true)
      monitored = spawn_exiting_on_stop(:kill)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      send(monitored, :stop)

      assert_receive {:EXIT, ^proxy, :kill}
    end

    test "passes an exit with reason :kill on without a crash report" do
      Process.flag(:trap_exit, true)
      monitored = spawn_exiting_on_stop(:kill)
      {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})

      log = capture_log(fn -> send_stop_and_await_proxy_exit(monitored, proxy, :kill) end)

      refute log =~ "terminating"
      refute log =~ "[error]"
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

    test "logs the name, the node and the delay when it finds the process gone" do
      Process.flag(:trap_exit, true)
      dead = spawn_dead_process()

      log = capture_log(fn -> start_proxy_and_await_exit(dead, :noproc) end)

      expected =
        "[info] SingletonProxy: name points at a process that is gone, looking it up again: name=#{inspect(@name)} node=#{inspect(node())} delay_ms=0"

      assert log =~ expected
    end

    test "proxies a process registered under the name during the failover delay and passes its exit on" do
      Process.flag(:trap_exit, true)
      registered_name = {:handler, make_ref()}
      name = {:via, :syn, {@scope, registered_name}}
      dead = spawn_dead_process()
      {:ok, proxy} = SingletonProxy.start_link(dead, name, {5_000, 5_000})
      new_holder = spawn_exiting_on_stop(:boom)
      on_exit(fn -> Process.exit(new_holder, :kill) end)

      :ok = :syn.register(@scope, registered_name, new_holder)

      await_monitor(new_holder, proxy)
      send(new_holder, :stop)
      assert_receive {:EXIT, ^proxy, :boom}
    end

    test "proxies a process the name already resolves to when it finds the holder gone, even with no failover delay" do
      Process.flag(:trap_exit, true)
      registered_name = {:handler, make_ref()}
      name = {:via, :syn, {@scope, registered_name}}
      new_holder = spawn_exiting_on_stop(:boom)
      on_exit(fn -> Process.exit(new_holder, :kill) end)
      :ok = :syn.register(@scope, registered_name, new_holder)
      dead = spawn_dead_process()

      {:ok, proxy} = SingletonProxy.start_link(dead, name, {0, 0})

      await_monitor(new_holder, proxy)
      send(new_holder, :stop)
      assert_receive {:EXIT, ^proxy, :boom}
    end

    test "logs the name, the node and the pid of a process it proxies after the holder was gone" do
      Process.flag(:trap_exit, true)
      registered_name = {:handler, make_ref()}
      name = {:via, :syn, {@scope, registered_name}}
      dead = spawn_dead_process()
      {:ok, proxy} = SingletonProxy.start_link(dead, name, {5_000, 5_000})
      new_holder = spawn_exiting_on_stop(:boom)
      on_exit(fn -> Process.exit(new_holder, :kill) end)

      log =
        capture_log(fn ->
          :ok = :syn.register(@scope, registered_name, new_holder)
          await_monitor(new_holder, proxy)
          Process.sleep(50)
        end)

      expected =
        "[info] SingletonProxy: name taken by a new process, proxying it: name=#{inspect(name)} node=#{inspect(node())} pid=#{inspect(new_holder)}"

      assert log =~ expected
    end

    test "exits with :noproc at the deadline drawn for the first holder when the process it finds under the name is gone too" do
      Process.flag(:trap_exit, true)
      gone_holder = spawn_dead_process()
      gone_replacement = spawn_dead_process()
      replaced_at_ms = System.monotonic_time(:millisecond) + 300
      name = {:via, GoneReplacementRegistry, {gone_holder, gone_replacement, replaced_at_ms}}

      {:ok, proxy} = SingletonProxy.start_link(gone_holder, name, {600, 600})

      refute_receive {:EXIT, ^proxy, _reason}, 500
      assert_receive {:EXIT, ^proxy, :noproc}, 250
    end

    test "waits a new failover delay when a process it found under the name exits with :noproc after the first deadline" do
      Process.flag(:trap_exit, true)
      registered_name = {:handler, make_ref()}
      name = {:via, :syn, {@scope, registered_name}}
      new_holder = spawn_exiting_on_stop(:noproc)
      on_exit(fn -> Process.exit(new_holder, :kill) end)
      :ok = :syn.register(@scope, registered_name, new_holder)
      dead = spawn_dead_process()
      {:ok, proxy} = SingletonProxy.start_link(dead, name, {200, 200})
      await_monitor(new_holder, proxy)
      Process.sleep(300)

      send(new_holder, :stop)

      refute_receive {:EXIT, ^proxy, _reason}, 100
      assert_receive {:EXIT, ^proxy, :noproc}, 1_000
    end

    test "keeps waiting while the name resolves to the process that is gone, and exits with :noproc after the failover delay" do
      Process.flag(:trap_exit, true)
      dead = spawn_dead_process()

      {:ok, proxy} = SingletonProxy.start_link(dead, {:via, StaleRegistry, dead}, {100, 100})

      refute_receive {:EXIT, ^proxy, _reason}, 50
      assert_receive {:EXIT, ^proxy, :noproc}, 1_000
    end
  end

  describe "failover_reason?/1" do
    test "is true for :noconnection, which a lost node delivers" do
      assert SingletonProxy.failover_reason?(:noconnection)
    end

    test "is true for :noproc, which a holder that was gone before the proxy started delivers" do
      assert SingletonProxy.failover_reason?(:noproc)
    end

    test "is true for :shutdown, which a stopping node gives its handlers" do
      assert SingletonProxy.failover_reason?(:shutdown)
    end

    test "is true for :killed, which a handler gets when its node's shutdown timeout runs out" do
      assert SingletonProxy.failover_reason?(:killed)
    end

    test "is true for the reason a singleton that lost a name conflict stops with" do
      assert SingletonProxy.failover_reason?({:shutdown, :name_conflict})
    end

    test "is false for :normal" do
      refute SingletonProxy.failover_reason?(:normal)
    end

    test "is false for {:shutdown, reason} with any reason other than a name conflict" do
      refute SingletonProxy.failover_reason?({:shutdown, :rejected})
    end

    test "is false for a crash reason" do
      refute SingletonProxy.failover_reason?(:crashed)
      refute SingletonProxy.failover_reason?({:rejected, "event"})
    end
  end

  describe "draw_failover_delay/1" do
    test "draws delays within the bounds and reaches both of them" do
      delays =
        fn -> SingletonProxy.draw_failover_delay({150, 200}) end
        |> Stream.repeatedly()
        |> Enum.take(1_000)

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

  # The pause gives the proxy's exit time to be logged, if it were.
  defp stop_monitored_and_await_proxy_exit(monitored, proxy, reason) do
    Process.exit(monitored, reason)
    assert_receive {:EXIT, ^proxy, ^reason}
    Process.sleep(50)
  end

  defp send_stop_and_await_proxy_exit(monitored, proxy, reason) do
    send(monitored, :stop)
    assert_receive {:EXIT, ^proxy, ^reason}
    Process.sleep(50)
  end

  defp start_proxy_and_await_exit(monitored, reason) do
    {:ok, proxy} = SingletonProxy.start_link(monitored, @name, {0, 0})
    assert_receive {:EXIT, ^proxy, ^reason}
    Process.sleep(50)
  end

  # Waits half a second at most, a tenth of the 5-second failover delay in
  # the tests that check the proxy looks the name up during the delay.
  defp await_monitor(monitored, proxy) do
    Polling.await(500, fn ->
      {:monitored_by, monitors} = Process.info(monitored, :monitored_by)

      if proxy in monitors,
        do: {:ok, :ok},
        else: {:error, "#{inspect(proxy)} does not monitor #{inspect(monitored)}"}
    end)
  end

  # `Process.exit(pid, :kill)` would end the process with `:killed`; exiting
  # from inside keeps `:kill`.
  defp spawn_exiting_on_stop(reason) do
    spawn(fn ->
      receive do
        :stop -> exit(reason)
      end
    end)
  end
end
