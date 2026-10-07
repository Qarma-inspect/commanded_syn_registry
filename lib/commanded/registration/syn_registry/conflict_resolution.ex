defmodule Commanded.Registration.SynRegistry.ConflictResolution do
  @moduledoc """
  The syn event handler the adapter installs, and the rule it applies when two
  nodes hold the same name in the scope of a Commanded application.

  syn keeps one event handler for the whole node. Starting a Commanded
  application with `Commanded.Registration.SynRegistry` installs this module
  as that handler and records the application's scope as one the adapter
  owns. The rule below applies only in those scopes, and `adapter_scope?/1`
  tells them apart from the rest.

  ## Conflicts in the adapter's scopes

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
  `{:shutdown, :name_conflict}`. A registration without the kind, made by a
  node that runs an older version of the adapter, is stopped with `:normal`.

  ## Every other scope

  A scope the adapter did not create behaves as it would without the
  adapter. When another event handler was installed at the moment the adapter
  installed this module, each callback for such a scope goes to that handler,
  if it exports the callback. Without one, syn's own rule decides a conflict:
  the registration made last wins, equal registration times go to the
  greater pid, and the other process is sent an exit signal with the reason
  `{:syn_resolve_kill, name, metadata}` by its own node.

  ## An event handler installed after the adapter

  A host application that installs its own handler once a Commanded
  application has started replaces this module in every scope. The adapter
  keeps working when that handler passes the callbacks of the adapter's
  scopes to `resolve_registry_conflict/4` and `on_process_unregistered/5`:

      alias Commanded.Registration.SynRegistry.ConflictResolution

      def resolve_registry_conflict(scope, name, entry, other_entry) do
        if ConflictResolution.adapter_scope?(scope),
          do: ConflictResolution.resolve_registry_conflict(scope, name, entry, other_entry),
          else: resolve_host_conflict(scope, name, entry, other_entry)
      end

      def on_process_unregistered(scope, name, pid, metadata, reason) do
        if ConflictResolution.adapter_scope?(scope),
          do: ConflictResolution.on_process_unregistered(scope, name, pid, metadata, reason),
          else: :ok
      end

  The handler has to export both callbacks: syn kills the loser of a
  conflict itself when the installed handler does not export
  `resolve_registry_conflict/4`. The adapter needs no other callback.
  """

  @behaviour :syn_event_handler

  require Logger

  @earlier_event_handler_key {__MODULE__, :earlier_event_handler}

  # The reason a singleton that loses its name in a conflict stops with.
  @singleton_conflict_loss_reason {:shutdown, :name_conflict}

  # What every registration made by the adapter carries: the time the process
  # was started, in nanoseconds of its node's system clock, and whether it is
  # a singleton or an aggregate.
  @typedoc false
  @type registration_kind :: :singleton | :aggregate

  @typedoc false
  @type registration_metadata :: %{started_at: integer(), kind: registration_kind()}

  @doc """
  Returns whether `scope` is the syn scope of a Commanded application that
  was started with this adapter on this node.

  An event handler installed after the adapter uses it to pick the callbacks
  it passes to this module.
  """
  @spec adapter_scope?(scope :: atom()) :: boolean()
  def adapter_scope?(scope), do: :persistent_term.get({__MODULE__, :adapter_scope, scope}, false)

  # Called by `Commanded.Registration.SynRegistry.child_spec/2` for every
  # Commanded application, before the node joins the scope. The handler this
  # module replaces is read first, and the scope is recorded before syn can
  # report anything from it.
  @doc false
  @spec install_for_scope(scope :: atom()) :: :ok
  def install_for_scope(scope) do
    record_earlier_event_handler()
    :persistent_term.put({__MODULE__, :adapter_scope, scope}, true)

    :syn.set_event_handler(__MODULE__)
  end

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

  In a scope of the adapter both nodes run this and must agree, so the answer
  comes from the two entries alone: the older `:started_at` wins, and equal
  timestamps are broken by term order on the pids. In any other scope the
  event handler installed before the adapter answers when it exports this
  callback, and syn's own rule answers otherwise.
  """
  @impl :syn_event_handler
  def resolve_registry_conflict(scope, name, entry, other_entry) do
    event_handler = fetch_earlier_event_handler()

    cond do
      adapter_scope?(scope) -> keep_first_started(entry, other_entry)
      resolves_conflicts?(event_handler) -> event_handler.resolve_registry_conflict(scope, name, entry, other_entry)
      true -> keep_last_registered(entry, other_entry)
    end
  end

  @doc """
  Stops a process that has just lost its name in a conflict in one of the
  adapter's scopes.

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

  In any other scope it does what syn does without the adapter. Unless the
  event handler installed before the adapter resolves conflicts, it sends the
  local loser of a conflict the exit signal
  `{:syn_resolve_kill, name, metadata}`. Then it passes the call on to that
  handler when the handler exports this callback.
  """
  @impl :syn_event_handler
  def on_process_unregistered(scope, name, pid, metadata, reason) do
    if adapter_scope?(scope),
      do: stop_conflict_loser(scope, name, pid, metadata, reason),
      else: unregister_without_adapter(scope, name, pid, metadata, reason)
  end

  # syn calls the callbacks below in every scope. The adapter's scopes need
  # none of them, and in any other scope they reach the event handler
  # installed before the adapter.

  @doc false
  @impl :syn_event_handler
  def on_process_registered(scope, name, pid, metadata, reason) do
    arguments = [scope, name, pid, metadata, reason]

    pass_to_earlier_event_handler(scope, :on_process_registered, arguments)
  end

  @doc false
  @impl :syn_event_handler
  def on_registry_process_updated(scope, name, pid, metadata, reason) do
    arguments = [scope, name, pid, metadata, reason]

    pass_to_earlier_event_handler(scope, :on_registry_process_updated, arguments)
  end

  @doc false
  @impl :syn_event_handler
  def on_registry_process_updated(scope, name, pid, previous_metadata, metadata, reason) do
    arguments = [scope, name, pid, previous_metadata, metadata, reason]

    pass_to_earlier_event_handler(scope, :on_registry_process_updated, arguments)
  end

  @doc false
  @impl :syn_event_handler
  def on_process_joined(scope, group, pid, metadata, reason) do
    arguments = [scope, group, pid, metadata, reason]

    pass_to_earlier_event_handler(scope, :on_process_joined, arguments)
  end

  @doc false
  @impl :syn_event_handler
  def on_group_process_updated(scope, group, pid, metadata, reason) do
    arguments = [scope, group, pid, metadata, reason]

    pass_to_earlier_event_handler(scope, :on_group_process_updated, arguments)
  end

  @doc false
  @impl :syn_event_handler
  def on_group_process_updated(scope, group, pid, previous_metadata, metadata, reason) do
    arguments = [scope, group, pid, previous_metadata, metadata, reason]

    pass_to_earlier_event_handler(scope, :on_group_process_updated, arguments)
  end

  @doc false
  @impl :syn_event_handler
  def on_process_left(scope, group, pid, metadata, reason) do
    arguments = [scope, group, pid, metadata, reason]

    pass_to_earlier_event_handler(scope, :on_process_left, arguments)
  end

  # A second Commanded application finds this module installed already and
  # keeps the handler the first one found.
  defp record_earlier_event_handler do
    case Application.get_env(:syn, :event_handler) do
      __MODULE__ -> :ok
      event_handler -> :persistent_term.put(@earlier_event_handler_key, event_handler)
    end
  end

  defp fetch_earlier_event_handler, do: :persistent_term.get(@earlier_event_handler_key, nil)

  defp resolves_conflicts?(event_handler), do: function_exported?(event_handler, :resolve_registry_conflict, 4)

  defp keep_first_started(
         {pid, %{started_at: started_at}, _time},
         {other_pid, %{started_at: other_started_at}, _other_time}
       ) do
    cond do
      started_at < other_started_at -> pid
      other_started_at < started_at -> other_pid
      true -> max(pid, other_pid)
    end
  end

  # syn's own rule, from `syn_event_handler:do_resolve_registry_conflict/4`:
  # the registration with the later time wins, and equal times go to the
  # greater pid.
  defp keep_last_registered({pid, _metadata, time}, {other_pid, _other_metadata, other_time}) do
    cond do
      time > other_time -> pid
      time < other_time -> other_pid
      true -> max(pid, other_pid)
    end
  end

  # Every node that held the entry gets this call; only the owner stops the
  # process.
  defp stop_conflict_loser(scope, name, pid, metadata, :syn_conflict_resolution)
       when node(pid) == node() do
    log_conflict_loss(scope, name, pid)

    spawn(GenServer, :stop, [pid, choose_stop_reason(metadata)])

    :ok
  end

  defp stop_conflict_loser(_scope, _name, _pid, _metadata, _reason), do: :ok

  # A node that runs an older version of the adapter registers names without
  # the kind and stops every loser with `:normal`.
  defp choose_stop_reason(%{kind: :singleton}), do: @singleton_conflict_loss_reason
  defp choose_stop_reason(_metadata), do: :normal

  # syn sends the kill signal itself, from the loser's own node and right
  # before it reports the loss, whenever the installed handler leaves
  # conflicts to syn. Installed in its place, this module sends it instead.
  defp unregister_without_adapter(scope, name, pid, metadata, reason) do
    event_handler = fetch_earlier_event_handler()

    if kill_conflict_loser?(event_handler, pid, reason), do: Process.exit(pid, {:syn_resolve_kill, name, metadata})

    call_if_exported(event_handler, :on_process_unregistered, [scope, name, pid, metadata, reason])
  end

  defp kill_conflict_loser?(event_handler, pid, :syn_conflict_resolution),
    do: node(pid) == node() and not resolves_conflicts?(event_handler)

  defp kill_conflict_loser?(_event_handler, _pid, _reason), do: false

  defp pass_to_earlier_event_handler(scope, callback, arguments) do
    if adapter_scope?(scope),
      do: :ok,
      else: call_if_exported(fetch_earlier_event_handler(), callback, arguments)
  end

  defp call_if_exported(event_handler, callback, arguments) do
    if function_exported?(event_handler, callback, length(arguments)),
      do: apply(event_handler, callback, arguments),
      else: :ok
  end

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
