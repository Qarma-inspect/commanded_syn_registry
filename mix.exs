defmodule Commanded.Registration.SynRegistry.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/Qarma-inspect/commanded_syn_registry"

  def project do
    [
      app: :commanded_syn_registry,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      source_url: @source_url,
      docs: docs()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp description do
    "A Commanded registry adapter built on syn, for clustered Commanded applications."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_url: @source_url,
      source_ref: "v" <> @version
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:commanded, ">= 1.4.11 and < 2.0.0"},
      {:syn, git: "https://github.com/Qarma-inspect/syn.git", ref: "1b4a03051cc60d0695bae1ccb7f397d99a8c7190"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end
end
