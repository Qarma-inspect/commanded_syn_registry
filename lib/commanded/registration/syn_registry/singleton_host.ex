defmodule Commanded.Registration.SynRegistry.SingletonHost do
  # Hosts one cluster singleton on one node.
  #
  # `Commanded.Registration.SynRegistry` starts a host for every event handler
  # and process router, on every node, and Commanded's supervision tree
  # supervises the host in the handler's place. The host runs one child and
  # decides at each start what it is: the singleton itself, registered under
  # its syn name, or a `Commanded.Registration.SynRegistry.SingletonProxy`
  # that monitors the process already holding that name.
  #
  # The host starts its child again only for registry churn: the singleton
  # lost its name in a conflict, or the proxy exited with a `SingletonProxy`
  # failover reason because the process holding the name went down with its
  # node, was stopped by it, had already gone, or lost a conflict. A restart
  # that syn keeps refusing because the name still points at a process that
  # has exited is churn as well, and the host tries again until the name
  # clears. A conflict round or a failover wave touches every handler on the
  # node at once, and these restarts stay out of the restart budget of the
  # application's supervision tree. They are counted against a budget of the
  # host's own instead.
  #
  # Every other exit of the singleton ends the host with the singleton's exit
  # reason, on the node that ran the singleton and, through the proxies, on
  # every node that proxied it. The parent's restart type and restart budget
  # therefore apply to the handler on every node, as if each parent were
  # linked to it, apart from an exit a proxy did not see: a proxy started
  # for a name that still pointed at the exited singleton monitors the
  # replacement once a lookup finds it, and misses an exit before that. A
  # crash, a crash loop and a stop the handler asks for all reach the
  # parents unchanged. A singleton that exits with a failover reason itself,
  # a bare `:shutdown` or `:killed` for instance, reaches only the parent on
  # its own node, and with `{:shutdown, :name_conflict}` no parent at all;
  # the other nodes treat it as if its node had stopped it.
  #
  # The parent's shutdown value decides how long the singleton gets to stop.
  # On shutdown the host passes `:shutdown` to its child and waits for it with
  # no limit of its own. When the parent's shutdown value runs out, or under
  # `:brutal_kill`, the parent kills the host, and the host's
  # `Commanded.Registration.SynRegistry.SingletonReaper` kills the singleton.

  @moduledoc false

  use GenServer

  require Logger

  alias Commanded.Registration.SynRegistry.ConflictResolution
  alias Commanded.Registration.SynRegistry.SingletonProxy
  alias Commanded.Registration.SynRegistry.SingletonReaper
  alias Commanded.Registration.SynRegistry.VanishedHolder

  @typedoc """
  The registry adapter metadata the host reads: the syn scope its name lives
  in and the bounds of the delay a proxy waits before it exits.
  """
  @type adapter_meta :: %{
          :scope => atom(),
          :failover_delay_range => SingletonProxy.failover_delay_range(),
          optional(atom()) => term()
        }

  @typedoc """
  What the host's child is: the singleton registered under the name, or a
  proxy for the process that holds it.
  """
  @type child_role :: :singleton | :proxy

  @typedoc """
  The times the host started its child again for registry churn within the
  current budget window, newest first, in milliseconds of the monotonic clock.
  """
  @type registry_restart_times :: [integer()]

  @enforce_keys [:scope, :name, :module, :args, :start_opts, :failover_delay_range]
  defstruct [
    :scope,
    :name,
    :module,
    :args,
    :start_opts,
    :failover_delay_range,
    :child,
    :child_role,
    :reaper,
    :reaper_ref,
    registry_restarts: []
  ]

  @type t :: %__MODULE__{
          scope: atom(),
          name: term(),
          module: module(),
          args: term(),
          start_opts: GenServer.options(),
          failover_delay_range: SingletonProxy.failover_delay_range(),
          child: pid() | nil,
          child_role: child_role() | nil,
          reaper: pid() | nil,
          reaper_ref: reference() | nil,
          registry_restarts: registry_restart_times()
        }

  # While the syn name still points at a pid on a node that has gone away, the
  # proxy exits and the host starts its child again once per failover delay,
  # which is five times a second with the default 200 ms minimum. The budget
  # sits above that rate, so the loop runs until syn drops the stale entry.
  # A restart that syn refuses because the name still points at a local
  # process that has exited comes round about every 45 ms, five attempts
  # 10 ms apart, so thirty of them take about 1.4 seconds. syn clears such an
  # entry within milliseconds, so this loop uses up the budget only when syn
  # keeps refusing far longer than that. A host that restarts for churn more
  # than 30 times in 5 seconds stops and leaves the decision to its parent.
  @max_registry_restarts 30
  @registry_restart_window_ms 5_000

  @doc """
  Starts the host for the singleton registered under `name`.

  The host itself carries no name; `name` belongs to the singleton it runs.
  The start fails when the singleton can be neither started nor proxied.
  """
  @spec start_link(
          adapter_meta(),
          name :: term(),
          singleton_module :: module(),
          init_arg :: term(),
          start_opts :: GenServer.options()
        ) :: GenServer.on_start()
  def start_link(adapter_meta, name, module, args, start_opts) do
    GenServer.start_link(__MODULE__, {adapter_meta, name, module, args, start_opts})
  end

  @impl GenServer
  def init({adapter_meta, name, module, args, start_opts}) do
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      scope: Map.fetch!(adapter_meta, :scope),
      name: name,
      module: module,
      args: args,
      start_opts: start_opts,
      failover_delay_range: Map.fetch!(adapter_meta, :failover_delay_range)
    }

    state
    |> start_reaper()
    |> start_child()
    |> case do
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:stop, {:shutdown, {:failed_to_start_child, module, reason}}}
    end
  end

  # The host answers `Supervisor.which_children/1` and
  # `Supervisor.count_children/1` like a supervisor with one worker, so code
  # that walks the supervision tree finds the singleton or its proxy. It
  # refuses every other request.
  @impl GenServer
  def handle_call(:which_children, _from, %__MODULE__{} = state), do: {:reply, [describe_child(state)], state}

  def handle_call(:count_children, _from, %__MODULE__{} = state), do: {:reply, count_children(state), state}

  def handle_call(_request, _from, %__MODULE__{} = state), do: {:reply, {:error, :not_supported}, state}

  @impl GenServer
  def handle_info({:EXIT, child, reason}, %__MODULE__{child: child} = state) do
    if registry_churn?(state, reason),
      do: restart_child_within_budget(state, reason),
      else: exit_with(state, reason)
  end

  def handle_info({:start_child_again, refusal}, %__MODULE__{child: nil} = state), do: restart_child_within_budget(state, refusal)

  def handle_info({:DOWN, ref, :process, _reaper, _reason}, %__MODULE__{reaper_ref: ref} = state),
    do: {:noreply, guard_with_new_reaper(state)}

  def handle_info(_message, %__MODULE__{} = state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, %__MODULE__{child: nil}), do: :ok
  def terminate(_reason, %__MODULE__{} = state), do: stop_child(state)

  # Commanded's event handlers and process routers start with `{:ok, pid}` or
  # an error, never `:ignore`.
  defp start_child(%__MODULE__{} = state) do
    SingletonReaper.announce_start(state.reaper)
    start = VanishedHolder.start_with_retry(fn -> start_singleton(state) end)
    report_start_to_reaper(state, start)

    case start do
      {:ok, singleton} ->
        {:ok, %__MODULE__{state | child: singleton, child_role: :singleton}}

      {:error, {:already_started, holder}} when is_pid(holder) ->
        lookup_name = {:via, :syn, {state.scope, state.name}}
        # The proxy's init only sets up a monitor and cannot fail.
        {:ok, proxy} = SingletonProxy.start_link(holder, lookup_name, state.failover_delay_range)

        {:ok, %__MODULE__{state | child: proxy, child_role: :proxy}}

      {:error, _reason} = error ->
        error
    end
  end

  defp start_reaper(%__MODULE__{} = state) do
    {reaper, reaper_ref} = SingletonReaper.start_monitor(state.scope, state.name)

    %__MODULE__{state | reaper: reaper, reaper_ref: reaper_ref}
  end

  # Starts a reaper in place of one that died and has it guard the singleton.
  defp guard_with_new_reaper(%__MODULE__{} = state) do
    state = start_reaper(state)
    guard_singleton(state)

    state
  end

  # The host may read the `DOWN` of a reaper killed by a code purge only
  # after its next start, so it checks the reaper before each start and
  # replaces a dead one, dropping that `DOWN`. A reaper that dies during the
  # start itself is replaced when the host reads its `DOWN` after the start
  # returns. Until then the new singleton is unguarded: a host killed inside
  # that window, which lasts about as long as the singleton's `init/1`,
  # leaves the singleton with its name until its current callback returns.
  defp replace_dead_reaper(%__MODULE__{} = state) do
    if Process.alive?(state.reaper) do
      state
    else
      Process.demonitor(state.reaper_ref, [:flush])
      start_reaper(state)
    end
  end

  # A proxy does not trap exits and dies with the host, so only the singleton
  # needs the reaper.
  defp guard_singleton(%__MODULE__{child_role: :singleton} = state), do: SingletonReaper.guard(state.reaper, state.child)

  defp guard_singleton(%__MODULE__{}), do: :ok

  # Ends the start announced to the reaper, guarding the singleton if the
  # start produced one.
  defp report_start_to_reaper(%__MODULE__{} = state, {:ok, singleton}), do: SingletonReaper.guard(state.reaper, singleton)

  defp report_start_to_reaper(%__MODULE__{} = state, {:error, _reason}), do: SingletonReaper.guard(state.reaper, nil)

  # The registration carries the time the process is started and marks it as
  # a singleton, which is what `ConflictResolution` reads when the same name
  # turns up on two nodes.
  defp start_singleton(%__MODULE__{} = state) do
    registration_name = {:via, :syn, {state.scope, state.name, ConflictResolution.build_registration_metadata(:singleton)}}
    registration_opts = Keyword.put(state.start_opts, :name, registration_name)

    GenServer.start_link(state.module, state.args, registration_opts)
  end

  # A proxy exits only after the process holding the name went down, and with
  # the same reason. It is churn when that reason means the holder's node is
  # lost or stopping, the name pointed at a process that was already gone, or
  # the holder lost a conflict; any other reason is the singleton's own exit,
  # which the proxy passes on. An exit of the singleton is churn only when it
  # lost its name in a conflict.
  defp registry_churn?(%__MODULE__{child_role: :proxy}, reason), do: SingletonProxy.failover_reason?(reason)

  defp registry_churn?(%__MODULE__{child_role: :singleton}, reason), do: ConflictResolution.singleton_conflict_loss?(reason)

  defp restart_child_within_budget(%__MODULE__{} = state, reason) do
    now_ms = System.monotonic_time(:millisecond)
    recent_restarts = Enum.filter(state.registry_restarts, &within_restart_window?(&1, now_ms))
    registry_restarts = [now_ms | recent_restarts]
    state = %__MODULE__{state | registry_restarts: registry_restarts}

    if length(registry_restarts) > @max_registry_restarts,
      do: stop_after_too_many_restarts(state),
      else: start_child_again(state, reason)
  end

  defp within_restart_window?(restarted_at_ms, now_ms), do: now_ms - restarted_at_ms < @registry_restart_window_ms

  defp start_child_again(%__MODULE__{} = state, reason) do
    log_registry_restart(state, reason)
    state = replace_dead_reaper(state)

    case start_child(state) do
      {:ok, state} -> {:noreply, state}
      {:error, {:already_started, :undefined} = refusal} -> retry_refused_start(state, refusal)
      {:error, start_error} -> exit_with(state, start_error)
    end
  end

  # syn still refused the name after the attempts `VanishedHolder` makes, so
  # the host tries again as one more restart against the budget. It goes
  # through its mailbox first, so the parent's shutdown and supervisor calls
  # get through between attempts.
  defp retry_refused_start(%__MODULE__{} = state, refusal) do
    send(self(), {:start_child_again, refusal})

    {:noreply, %__MODULE__{state | child: nil, child_role: nil}}
  end

  defp stop_after_too_many_restarts(%__MODULE__{} = state) do
    log_too_many_restarts(state)

    exit_with(state, {:too_many_registry_restarts, state.name})
  end

  # Ends the host with an exit signal rather than `{:stop, reason, state}`, as
  # `SingletonProxy` does: `GenServer` logs a crash report for every stop
  # reason other than `:normal`, `:shutdown` or `{:shutdown, _}`, and the host
  # is not at fault. The signal ends the host with `reason` unchanged, so the
  # parent applies its restart type to the exit the singleton made. A process
  # that sends itself `:normal` exits as well once it no longer traps exits.
  # The child is gone by now, so skipping `terminate/2` leaves nothing behind.
  #
  # A signal the host sends itself with `:kill` would end it with `:killed`,
  # so the `:kill` signal comes from a linked process, as in the proxy.
  defp exit_with(%__MODULE__{} = state, :kill) do
    Process.flag(:trap_exit, false)
    spawn_link(fn -> exit(:kill) end)

    {:noreply, state}
  end

  defp exit_with(%__MODULE__{} = state, reason) do
    Process.flag(:trap_exit, false)
    Process.exit(self(), reason)

    {:noreply, state}
  end

  # Waits with no limit: the parent's shutdown value bounds the wait by
  # killing the host. Waits on a monitor rather than on the child's `EXIT`
  # message: when a callback raises after the host has taken that message, a
  # wait for it would never end.
  defp stop_child(%__MODULE__{child: child} = state) do
    child_ref = Process.monitor(child)
    Process.exit(child, :shutdown)

    await_child_down(state, child_ref)
  end

  # When the parent kills the host during the wait, only the reaper stops the
  # singleton, so the host replaces a reaper that dies meanwhile, from a code
  # purge for instance, and keeps waiting.
  defp await_child_down(%__MODULE__{child: child, reaper_ref: reaper_ref} = state, child_ref) do
    receive do
      {:DOWN, ^child_ref, :process, ^child, _reason} ->
        :ok

      {:DOWN, ^reaper_ref, :process, _reaper, _reason} ->
        state
        |> guard_with_new_reaper()
        |> await_child_down(child_ref)
    end
  end

  defp describe_child(%__MODULE__{child_role: :singleton} = state), do: {state.module, state.child, :worker, [state.module]}

  defp describe_child(%__MODULE__{child_role: :proxy} = state), do: {state.module, state.child, :worker, [SingletonProxy]}

  defp describe_child(%__MODULE__{child_role: nil} = state), do: {state.module, :restarting, :worker, [state.module]}

  # One worker, counted as active unless the host is between attempts to
  # start it again.
  defp count_children(%__MODULE__{child: nil}), do: [specs: 1, active: 0, supervisors: 0, workers: 1]

  defp count_children(%__MODULE__{}), do: [specs: 1, active: 1, supervisors: 0, workers: 1]

  defp log_registry_restart(%__MODULE__{} = state, reason) do
    [
      "SingletonHost: starting the child again after registry churn:",
      "name=#{inspect(state.name)}",
      "reason=#{inspect(reason)}"
    ]
    |> Enum.join(" ")
    |> Logger.debug()
  end

  defp log_too_many_restarts(%__MODULE__{} = state) do
    [
      "SingletonHost: too many registry restarts, stopping the host:",
      "name=#{inspect(state.name)}",
      "max_restarts=#{@max_registry_restarts}",
      "window_ms=#{@registry_restart_window_ms}"
    ]
    |> Enum.join(" ")
    |> Logger.error()
  end
end
