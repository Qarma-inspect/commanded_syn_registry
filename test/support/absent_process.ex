defmodule Commanded.Registration.SynRegistry.AbsentProcess do
  @moduledoc """
  Pids that name no running process, for tests of what the adapter does with
  a process that has already exited or that lives on a node out of reach.
  """

  import ExUnit.Assertions

  @doc """
  Builds a pid on `unreachable_node`, a node that is not running. It stands
  in for a process on another node: monitoring it reports `:noconnection` at
  once, the way a lost connection to the hosting node does.
  """
  @spec build_pid_on_unreachable_node(unreachable_node :: node()) :: pid()
  def build_pid_on_unreachable_node(unreachable_node) do
    node_name = Atom.to_string(unreachable_node)
    atom = <<100, byte_size(node_name)::16, node_name::binary>>

    :erlang.binary_to_term(<<131, 88, atom::binary, 1::32, 0::32, 1::32>>)
  end

  @doc """
  Spawns a process that exits at once, waits until it is gone and returns its
  pid.
  """
  @spec spawn_dead_process() :: pid()
  def spawn_dead_process do
    {pid, ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    pid
  end
end
