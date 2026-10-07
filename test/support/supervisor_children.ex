defmodule Commanded.Registration.SynRegistry.SupervisorChildren do
  @moduledoc """
  Finds the processes a supervisor runs, for tests that reach a singleton
  host, or the process it hosts, through the supervisor above it.
  """

  @typedoc """
  A child as `Supervisor.which_children/1` reports it: its pid, `:restarting`
  while the supervisor restarts it, or `:undefined` when no process runs for
  it.
  """
  @type child :: Supervisor.child() | :restarting

  @doc """
  Returns the only child of `supervisor`, such as the process a singleton
  host runs. Raises `MatchError` unless the supervisor has exactly one child.
  """
  @spec fetch_child(Supervisor.supervisor()) :: child()
  def fetch_child(supervisor) do
    [{_id, pid, _type, _modules}] = Supervisor.which_children(supervisor)

    pid
  end

  @doc """
  Returns the child of `supervisor` whose child spec lists `child_module`
  among its modules. Use it on a supervisor with several children, such as
  the top of a supervision tree. Raises `MatchError` when no child lists
  `child_module`.
  """
  @spec fetch_child(Supervisor.supervisor(), child_module :: module()) :: child()
  def fetch_child(supervisor, child_module) do
    {_id, pid, _type, _modules} =
      supervisor
      |> Supervisor.which_children()
      |> Enum.find(&child_of_module?(&1, child_module))

    pid
  end

  defp child_of_module?({_id, _pid, _type, modules}, module), do: module in modules
end
