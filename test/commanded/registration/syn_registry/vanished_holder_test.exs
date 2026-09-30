defmodule Commanded.Registration.SynRegistry.VanishedHolderTest do
  use ExUnit.Case, async: true

  alias Commanded.Registration.SynRegistry.VanishedHolder

  @refused {:error, {:already_started, :undefined}}

  describe "start_with_retry/1" do
    test "returns a successful start after one attempt" do
      attempts = script_attempts([{:ok, :pid}])

      assert VanishedHolder.start_with_retry(fn -> make_attempt(attempts) end) == {:ok, :pid}
      assert count_attempts(attempts) == 1
    end

    test "returns a start error that names a holder after one attempt" do
      holder_error = {:error, {:already_started, self()}}
      attempts = script_attempts([holder_error])

      assert VanishedHolder.start_with_retry(fn -> make_attempt(attempts) end) == holder_error
      assert count_attempts(attempts) == 1
    end

    test "returns any other start error after one attempt" do
      attempts = script_attempts([{:error, :boom}])

      assert VanishedHolder.start_with_retry(fn -> make_attempt(attempts) end) == {:error, :boom}
      assert count_attempts(attempts) == 1
    end

    test "attempts again until the start no longer names a vanished holder" do
      attempts = script_attempts([@refused, @refused, @refused, @refused, {:ok, :pid}])

      assert VanishedHolder.start_with_retry(fn -> make_attempt(attempts) end) == {:ok, :pid}
      assert count_attempts(attempts) == 5
    end

    test "gives up after five attempts and returns the last refusal" do
      attempts = script_attempts([@refused])

      assert VanishedHolder.start_with_retry(fn -> make_attempt(attempts) end) == @refused
      assert count_attempts(attempts) == 5
    end

    test "pauses at least 10 ms between attempts" do
      attempts = script_attempts([@refused, @refused, {:ok, :pid}])
      started_at = System.monotonic_time(:millisecond)

      VanishedHolder.start_with_retry(fn -> make_attempt(attempts) end)

      assert System.monotonic_time(:millisecond) - started_at >= 20
    end
  end

  # Attempt n returns the n-th of `results`, and every attempt after the
  # last result returns that result again.
  defp script_attempts(results) do
    {:ok, attempts} = Agent.start_link(fn -> {results, 0} end)

    attempts
  end

  defp make_attempt(attempts), do: Agent.get_and_update(attempts, &take_next_result/1)

  defp take_next_result({[result], count}), do: {result, {[result], count + 1}}
  defp take_next_result({[result | later], count}), do: {result, {later, count + 1}}

  defp count_attempts(attempts), do: Agent.get(attempts, &elem(&1, 1))
end
