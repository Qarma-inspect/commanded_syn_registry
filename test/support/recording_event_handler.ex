defmodule Commanded.Registration.SynRegistry.RecordingEventHandler do
  @moduledoc """
  A syn event handler that stands for one a host application installs.

  Every callback sends `{RecordingEventHandler, callback, arguments}` to the
  process registered under this module's name: the test process in the unit
  tests, and the recorder `ClusterTestNode.start_recording/0` starts on a
  peer. A registry conflict goes to the registration made first, the opposite
  of syn's own rule, so a test can tell which of the two decided it.
  """

  @behaviour :syn_event_handler

  @impl :syn_event_handler
  def on_process_registered(scope, name, pid, metadata, reason),
    do: report(:on_process_registered, [scope, name, pid, metadata, reason])

  @impl :syn_event_handler
  def on_registry_process_updated(scope, name, pid, metadata, reason),
    do: report(:on_registry_process_updated, [scope, name, pid, metadata, reason])

  @impl :syn_event_handler
  def on_registry_process_updated(scope, name, pid, previous_metadata, metadata, reason) do
    report(:on_registry_process_updated, [scope, name, pid, previous_metadata, metadata, reason])
  end

  @impl :syn_event_handler
  def on_process_unregistered(scope, name, pid, metadata, reason),
    do: report(:on_process_unregistered, [scope, name, pid, metadata, reason])

  @impl :syn_event_handler
  def on_process_joined(scope, group, pid, metadata, reason),
    do: report(:on_process_joined, [scope, group, pid, metadata, reason])

  @impl :syn_event_handler
  def on_group_process_updated(scope, group, pid, metadata, reason),
    do: report(:on_group_process_updated, [scope, group, pid, metadata, reason])

  @impl :syn_event_handler
  def on_group_process_updated(scope, group, pid, previous_metadata, metadata, reason) do
    report(:on_group_process_updated, [scope, group, pid, previous_metadata, metadata, reason])
  end

  @impl :syn_event_handler
  def on_process_left(scope, group, pid, metadata, reason), do: report(:on_process_left, [scope, group, pid, metadata, reason])

  @impl :syn_event_handler
  def resolve_registry_conflict(scope, name, entry, other_entry) do
    report(:resolve_registry_conflict, [scope, name, entry, other_entry])

    keep_first_registered(entry, other_entry)
  end

  defp keep_first_registered({pid, _metadata, time}, {other_pid, _other_metadata, other_time}) do
    cond do
      time < other_time -> pid
      other_time < time -> other_pid
      true -> min(pid, other_pid)
    end
  end

  defp report(callback, arguments) do
    send(__MODULE__, {__MODULE__, callback, arguments})

    :ok
  end
end
