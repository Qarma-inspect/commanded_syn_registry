# The registry's cluster tests start peer nodes, which need epmd and a
# distributed test node for the pids they hand back. A VM that is already
# distributed, as with `elixir --sname name -S mix test`, keeps its own name.
System.cmd("epmd", ["-daemon"])

case Node.alive?() do
  true -> :ok
  false -> {:ok, _net_kernel} = Node.start(:"commanded_syn_registry_test@127.0.0.1", :longnames)
end

# Commanded logs every command and event at debug level, which buries the
# results, and its processes log again while the supervision tree shuts down
# after a test. Tests capture the info logs they assert on themselves, and the
# one test that asserts on a debug line raises the level for its own run.
Logger.configure(level: :info)
ExUnit.start(capture_log: true)
