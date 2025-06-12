defmodule Eliraft.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc """
  The Eliraft application.
  This module is responsible for starting the supervision tree.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: Eliraft.Worker.start_link(arg)
      # {Eliraft.Worker, arg}
      {Eliraft.Server, [table: :eliraft_table, partition: 1, application: Eliraft]},
      Eliraft.Supervisor
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Eliraft.ApplicationSupervisor]
    Supervisor.start_link(children, opts)
  end
end
