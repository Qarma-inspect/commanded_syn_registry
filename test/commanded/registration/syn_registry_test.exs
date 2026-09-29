defmodule Commanded.Registration.SynRegistryTest do
  use ExUnit.Case
  doctest Commanded.Registration.SynRegistry

  alias Commanded.Registration.SynRegistry

  test "greets the world" do
    assert SynRegistry.hello() == :world
  end
end
