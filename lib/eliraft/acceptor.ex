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

  @type read_error_type :: :not_leader | :read_queue_full | :apply_queue_full | {:notify_redirect, node()}
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
    :name,
    :server,
    :queues
  ]

  # Public API

  @doc """
  Starts a new acceptor server.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @doc """
  Commits an operation to the Raft log with a default timeout.
  """
  def commit(acceptor, op) do
    commit(acceptor, op, 5000)
  end

  @doc """
  Commits an operation with a custom timeout.
  """
  def commit(acceptor, op, timeout) do
    GenServer.call(acceptor, {:commit, op}, timeout)
  end

  @doc """
  Asynchronously commits an operation.
  """
  def commit_async(acceptor, op, timeout) do
    GenServer.cast(acceptor, {:commit, op, timeout})
  end

  @doc """
  Performs a strong read operation with a default timeout.
  """
  def read(acceptor, op) do
    read(acceptor, op, 5000)
  end

  @doc """
  Performs a strong read operation with a custom timeout.
  """
  def read(acceptor, op, timeout) do
    GenServer.call(acceptor, {:read, op}, timeout)
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    Logger.notice("Acceptor[#{opts[:name]}] starting")
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      name: opts[:name],
      server: opts[:server],
      queues: opts[:queues]
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:commit, op}, _from, state) do
    # Check if the command is valid
    if not is_valid_command?(op) do
      {:reply, {:error, :invalid_command}, state}
    else
      # Forward the commit to the server
      case GenServer.call(state.server, {:append, %{term: 1, command: op}}) do
        :ok -> {:reply, :ok, state}
        error -> {:reply, error, state}
      end
    end
  end

  @impl true
  def handle_call({:read, _op}, _from, state) do
    # TODO: Implement read logic
    {:reply, {:ok, nil}, state}
  end

  @impl true
  def handle_cast({:commit, _op, _timeout}, state) do
    # TODO: Implement async commit logic
    {:noreply, state}
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