defmodule Commanded.Registration.SynRegistry.VanishedHolder do
  @moduledoc false

  # syn refuses a registration when the name is taken. The adapter then looks
  # up the holder to report it, and that lookup returns `:undefined` when the
  # holder has died in between: a local process that is no longer alive counts
  # as unregistered even while its entry is still in the table. The start then
  # fails with `{:error, {:already_started, :undefined}}`, an error that names
  # no process to fall back on.
  #
  # The entry that caused the refusal disappears when the scope process
  # handles the holder's `DOWN` message (`syn_registry:handle_info/2`). For a
  # local holder that message is already on its way, so the name frees up as
  # soon as the scope process reaches it in its mailbox, a matter of
  # milliseconds. A short pause and a new start attempt therefore finds the
  # name free. A holder on another node is never reported as `:undefined`,
  # because syn cannot check a remote process for liveness, so no wait longer
  # than a few milliseconds is ever useful here.
  @max_attempts 5
  @pause_between_attempts_ms 10

  @doc """
  Calls `start_attempt`, a function that makes one attempt to start the
  process and returns what the start returned, up to five times, pausing 10 ms
  between attempts, for as long as it fails with
  `{:error, {:already_started, :undefined}}`.

  Any other result, and the last refusal once the attempts are used up, is
  returned as it is.
  """
  @spec start_with_retry((-> result)) :: result when result: term()
  def start_with_retry(start_attempt) when is_function(start_attempt, 0),
    do: start_with_retry(start_attempt, @max_attempts)

  defp start_with_retry(start_attempt, attempts_left) do
    case start_attempt.() do
      {:error, {:already_started, :undefined}} when attempts_left > 1 ->
        Process.sleep(@pause_between_attempts_ms)
        start_with_retry(start_attempt, attempts_left - 1)

      result ->
        result
    end
  end
end
