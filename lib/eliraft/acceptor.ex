defmodule Eliraft.Acceptor do
  @moduledoc """
  Handles client requests for Raft operations.
  This module implements the front-end process for accepting commits and reads.
  """

  use GenServer
  require Logger

  alias Eliraft.{Queue, Config, Server}

  # Types
  @type command :: term()
  @type key :: term()
  @type op :: term()
  @type read_op :: term()

  @type call_error_type :: :timeout | :unreachable | {:call_error, term()}
  @type call_error :: {:error, call_error_type()}
  @type call_result :: term() | call_error()

  @type read_error_type ::
          :not_leader | :read_queue_full | :apply_queue_full | {:notify_redirect, node()}
  @type read_error :: {:error, read_error_type()}
  @type read_result :: term() | read_error() | call_error()

  @type commit_error_type ::
          :not_leader
          | {:duplicate_request, key()}
          | {:commit_queue_full, key()}
          | {:apply_queue_full, key()}
          | {:notify_redirect, node()}
          | :commit_stalled
          | :invalid_command
  @type commit_error :: {:error, commit_error_type()}
  @type commit_result :: term() | commit_error() | call_error()

  # Server state
  defstruct [
    :server,
    :storage,
    :state
  ]

  # Public API

  @doc """
  Starts a new acceptor server.
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Commits an operation to the Raft log with a default timeout.
  """
  def commit(acceptor, command) do
    GenServer.call(acceptor, {:commit, command})
  end

  @doc """
  Commits an operation with a custom timeout.
  """
  def commit(acceptor, command, timeout) do
    GenServer.call(acceptor, {:commit, command}, timeout)
  end

  @doc """
  Asynchronously commits an operation.
  """
  def commit_async(acceptor, command, timeout) do
    GenServer.cast(acceptor, {:commit, command, timeout})
  end

  @doc """
  Performs a strong read operation with a default timeout.
  """
  def read(acceptor, key) do
    GenServer.call(acceptor, {:read, key})
  end

  @doc """
  Performs a strong read operation with a custom timeout.
  """
  def read(acceptor, key, timeout) do
    GenServer.call(acceptor, {:read, key}, timeout)
  end

  def set_server(acceptor, server) do
    GenServer.call(acceptor, {:set_server, server})
  end

  @doc """
  Retrieves a value from the storage.
  """
  def get(acceptor) do
    GenServer.call(acceptor, :get)
  end

  @doc """
  Puts a value into the storage.
  """
  def put(acceptor, key, value) do
    GenServer.call(acceptor, {:put, key, value})
  end

  @doc """
  Deletes a value from the storage.
  """
  def delete(acceptor, key) do
    GenServer.call(acceptor, {:delete, key})
  end

  @doc """
  Clears the storage.
  """
  def clear(acceptor) do
    GenServer.call(acceptor, :clear)
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    state = %__MODULE__{
      server: nil,
      storage: Keyword.get(opts, :storage),
      state: :follower
    }
    {:ok, state}
  end

  @impl true
  def handle_call({:set_server, server}, _from, state) do
    {:reply, :ok, %{state | server: server}}
  end

  @impl true
  def handle_call({:commit, command}, _from, state) do
    case Server.get_state(state.server) do
      :leader ->
        case is_valid_command?(command) do
          true ->
            case Server.append(state.server, %{term: Server.get_term(state.server), command: command}) do
              :ok -> {:reply, :ok, state}
              error -> {:reply, error, state}
            end
          false ->
            {:reply, {:error, :invalid_command}, state}
        end
      _ ->
        {:reply, {:error, :not_leader}, state}
    end
  end

  @impl true
  def handle_call({:read, key}, _from, state) do
    case Eliraft.Storage.read(state.storage, key) do
      {:ok, value} -> {:reply, {:ok, value}, state}
      {:error, :not_found} -> {:reply, {:ok, nil}, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(:get, _from, state) do
    {:reply, Eliraft.Storage.Disk.get(state.storage), state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    new_storage = Eliraft.Storage.Disk.put(state.storage, key, value)
    {:reply, :ok, %{state | storage: new_storage}}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    new_storage = Eliraft.Storage.Disk.delete(state.storage, key)
    {:reply, :ok, %{state | storage: new_storage}}
  end

  @impl true
  def handle_call(:clear, _from, state) do
    new_storage = Eliraft.Storage.Disk.clear(state.storage)
    {:reply, :ok, %{state | storage: new_storage}}
  end

  @impl true
  def handle_cast({:commit, command, timeout}, state) do
    case Server.get_state(state.server) do
      :leader ->
        case Server.append(state.server, %{term: Server.get_term(state.server), command: command}) do
          :ok -> {:noreply, state}
          _ -> {:noreply, state}
        end
      _ ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_call(request, _from, state) do
    Logger.warning("Unhandled call to Acceptor: #{inspect(request)}")
    {:reply, {:error, :unhandled_call}, state}
  end

  # Private Functions

  defp is_valid_command?(op) do
    case op do
      {:set, _key, _value} -> true
      {:delete, _key} -> true
      _ -> false
    end
  end

  defp make_call(_server, _request, _timeout) do
    # TODO: Implement call logic
    {:error, :timeout}
  end

  defp default_name(table, partition) do
    :"raft_acceptor_#{table}_#{partition}"
  end

  defp registered_name(table, partition) do
    # TODO: Implement registered name lookup
    default_name(table, partition)
  end
end
