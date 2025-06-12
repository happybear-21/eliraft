defmodule Eliraft.Supervisor do
  @moduledoc """
  Supervisor for Raft servers.
  This module is responsible for managing the lifecycle of Raft server processes.
  """

  use DynamicSupervisor

  @doc """
  Starts the supervisor.
  """
  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @doc """
  Starts a new Raft server under the supervisor.
  """
  def start_server(args) do
    case DynamicSupervisor.start_child(__MODULE__, {Eliraft.Server, args}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      error -> error
    end
  end

  @doc """
  Stops a Raft server.
  """
  def stop_server(server) do
    DynamicSupervisor.terminate_child(__MODULE__, server)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_restarts: 3,
      max_seconds: 5
    )
  end
end 