defmodule Commanded.Registration.SynRegistry.ConflictResolution do
  @moduledoc """
  The syn event handler of every scope the adapter creates, and the rule it
  applies when two nodes hold the same name in such a scope.

  `Commanded.Registration.SynRegistry` installs this module with
  `:syn.set_event_handler/2` as the event handler of each Commanded
  application's scope, before the node joins the scope, and sets no other
  handler in syn. syn calls this module for everything that happens in that
  scope and for nothing else. Do not list the application's scope under
  syn's `scope_event_handlers` configuration key: the adapter writes over
  that entry when the Commanded application starts, and syn puts it back
  if the syn application restarts.

  ## Conflicts

  syn registers a name with a local write and a broadcast, so two nodes that
  cannot see each other can each register the same name. When they meet, syn
  detects the clash and asks this module which of the two processes to keep.
  It asks on both nodes that own the conflicting processes, each on its own,
  and the two answers must name the same process. The rule below therefore
  uses only what both nodes read in the two entries, never which of them is
  the local one.

  The process with the older `:started_at` wins. Every registration made by
  `Commanded.Registration.SynRegistry` carries that timestamp in its
  metadata, taken when the process is started, so the name stays with the
  process that has served it longest and the one started later gives it up.
  The timestamps come from the clocks of the two nodes, so the older
  registration wins only as far as those clocks agree. When they disagree by
  more than the gap between the two starts, the process that started later
  can keep the name. Both nodes still reach the same answer, because both
  read the same two timestamps. Two starts in the same nanosecond are decided
  by Erlang term order on the pids, which reads the same on both nodes.

  syn does not kill the loser here. It drops the entry from its table and
  calls `on_process_unregistered/5` with the reason `:syn_conflict_resolution`
  on every node that held the loser's entry. Only the loser's own node stops
  the process, with the reason its kind calls for; on any other node this
  module ignores the call. The registration metadata says whether the
  process is an aggregate, stopped with `:normal`, or a singleton, meaning
  an event handler or a process router, stopped with
  `{:shutdown, :name_conflict}`. A registration whose metadata has no `:kind`
  key is stopped with `:normal`, as an aggregate is.
  """

  @behaviour :syn_event_handler

  require Logger

  # The reason a singleton that loses its name in a conflict stops with.
  @singleton_conflict_loss_reason {:shutdown, :name_conflict}

  # What every registration made by the adapter carries: the time the process
  # was started, in nanoseconds of its node's system clock, and whether it is
  # a singleton or an aggregate.
  @typedoc false
  @type registration_kind :: :singleton | :aggregate

  @typedoc false
  @type registration_metadata :: %{started_at: integer(), kind: registration_kind()}

  # Called by `Commanded.Registration.SynRegistry.child_spec/2` for every
  # Commanded application, before the node joins the scope. Joining starts the
  # exchange of registrations with the other nodes, and a scope without a handler
  # of its own hands the callbacks of that exchange to a node-wide handler of the
  # host. A conflict needs a registration of this node's own, which the adapter
  # makes only after `child_spec/2` has returned, so the order is about those
  # callbacks, not about the rule. Calling it again for the same scope changes
  # nothing.
  @doc false
  @spec install_for_scope(scope :: atom()) :: :ok
  def install_for_scope(scope), do: :syn.set_event_handler(scope, __MODULE__)

  # Called by the adapter for every name it registers, with the time the
  # process is started.
  @doc false
  @spec build_registration_metadata(registration_kind()) :: registration_metadata()
  def build_registration_metadata(kind), do: %{started_at: System.system_time(:nanosecond), kind: kind}

  # Called by the process hosting a singleton when the singleton exits. A lost
  # conflict is registry churn, and the host starts the singleton again; any
  # other exit is the singleton's own.
  @doc false
  @spec singleton_conflict_loss?(exit_reason :: term()) :: boolean()
  def singleton_conflict_loss?(exit_reason), do: exit_reason == @singleton_conflict_loss_reason

  @doc """
  Returns the pid to keep for a name registered on two nodes at once.

  Both nodes run this and must agree, so the answer comes from the two
  entries alone: the older `:started_at` wins, and equal timestamps are
  broken by term order on the pids.
  """
  @impl :syn_event_handler
  def resolve_registry_conflict(
        _scope,
        _name,
        {pid, %{started_at: started_at}, _time},
        {other_pid, %{started_at: other_started_at}, _other_time}
      ) do
    cond do
      started_at < other_started_at -> pid
      other_started_at < started_at -> other_pid
      true -> max(pid, other_pid)
    end
  end

  @doc """
  Stops a process that has just lost its name in a conflict.

  syn calls this on every node that held the losing process's entry. Only the
  node that owns the process stops it, and on any other node the call does
  nothing. An aggregate stops with `:normal`, because Commanded's dispatcher
  retries a command only when the aggregate stopped normally or was not
  running. A singleton stops with
  `{:shutdown, :name_conflict}`, which `GenServer` does not report as a
  crash. The process that hosts the singleton on this node starts it again,
  and the new start finds the name taken and becomes a proxy for the process
  that won. A registration without the kind is stopped with `:normal`.

  The callback runs inside the scope process, which must keep serving
  registrations, so the stop runs in a process of its own.
  """
  @impl :syn_event_handler
  def on_process_unregistered(scope, name, pid, metadata, :syn_conflict_resolution)
      when node(pid) == node() do
    log_conflict_loss(scope, name, pid)

    spawn(GenServer, :stop, [pid, choose_stop_reason(metadata)])

    :ok
  end

  def on_process_unregistered(_scope, _name, _pid, _metadata, _reason), do: :ok

  # A registration whose metadata has no `:kind` key is stopped with
  # `:normal`, as an aggregate is.
  defp choose_stop_reason(%{kind: :singleton}), do: @singleton_conflict_loss_reason
  defp choose_stop_reason(_metadata), do: :normal

  defp log_conflict_loss(scope, name, pid) do
    [
      "SynRegistry: conflict lost, stopping the process:",
      "scope=#{inspect(scope)}",
      "name=#{inspect(name)}",
      "pid=#{inspect(pid)}"
    ]
    |> Enum.join(" ")
    |> Logger.info()
  end
end
