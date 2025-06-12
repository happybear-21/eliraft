defmodule Eliraft.Storage do
  @moduledoc """
  Manages Raft storage.
  This module provides functionality for handling the state machine replicated by Raft.
  """

  use GenServer
  require Logger

  alias Eliraft.{Log, Config}

  # Types
  @type storage_handle :: term()
  @type metadata :: %{
    position: non_neg_integer(),
    label: term()
  }
  @type error_reason :: :not_found | :invalid_position | :invalid_label | term()
  @type status :: :ok | {:error, error_reason()}

  defstruct [
    :name,
    :table,
    :partition,
    :provider,
    :handle,
    :position,
    :label,
    :config
  ]

  # Public API

  @doc """
  Starts a new storage server.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @doc """
  Gets the current status of the storage.
  """
  def status(server) do
    GenServer.call(server, :status)
  end

  @doc """
  Gets the current position of the storage.
  """
  def position(server) do
    GenServer.call(server, :position)
  end

  @doc """
  Gets the current label of the storage.
  """
  def label(server) do
    GenServer.call(server, :label)
  end

  @doc """
  Gets the current configuration of the storage.
  """
  def config(server) do
    GenServer.call(server, :config)
  end

  @doc """
  Reads a value from the storage.
  """
  def read(server, key) do
    GenServer.call(server, {:read, key})
  end

  @doc """
  Applies a command to storage.
  """
  def apply(server, command, position, label) do
    GenServer.call(server, {:apply, command, position, label})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    Logger.notice("Storage[#{opts[:name]}] starting")
    Process.flag(:trap_exit, true)

    state = %{
      metadata: %{position: 0, label: nil},
      config: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:position, _from, state) do
    {:reply, {:ok, state.metadata.position}, state}
  end

  @impl true
  def handle_call(:label, _from, state) do
    {:reply, {:ok, state.metadata.label}, state}
  end

  @impl true
  def handle_call(:config, _from, state) do
    {:reply, {:ok, state.config}, state}
  end

  @impl true
  def handle_call({:read, key}, _from, state) do
    case do_read(state, key) do
      {:ok, value} -> {:reply, {:ok, value}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:apply, command, position, label}, _from, state) do
    case do_apply(state, command, position, label) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # Private Functions

  defp do_read(state, key) do
    # TODO: Implement read logic
    {:error, :not_found}
  end

  defp do_apply(state, command, position, label) do
    # TODO: Implement apply logic
    {:error, :invalid_position}
  end

  defp get_metadata(_state) do
    # TODO: Implement metadata retrieval logic
    %{position: 0, label: nil}
  end
end 