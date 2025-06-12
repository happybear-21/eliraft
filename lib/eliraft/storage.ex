defmodule Eliraft.Storage do
  @moduledoc """
  The Raft storage implementation.
  """

  import Kernel, except: [apply: 2]
  use GenServer
  require Logger

  alias Eliraft.{Log, Config, Storage.Disk}

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
    :config,
    :disk_storage,
    :state_machine,
    :metadata,
    :disk,
    :state,
    :current_term
  ]

  # Public API

  @doc """
  Starts a new storage server.
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets the current status of the storage.
  """
  def status(storage) do
    GenServer.call(storage, :status)
  end

  @doc """
  Gets the current position of the storage.
  """
  def position(storage) do
    GenServer.call(storage, :position)
  end

  @doc """
  Gets the current label of the storage.
  """
  def label(storage) do
    GenServer.call(storage, :label)
  end

  @doc """
  Gets the current configuration of the storage.
  """
  def config(storage) do
    GenServer.call(storage, :config)
  end

  @doc """
  Reads a value from the storage.
  """
  def read(storage, key) do
    GenServer.call(storage, {:read, key})
  end

  @doc """
  Applies a command to storage.
  """
  def apply(storage, command) do
    GenServer.call(storage, {:apply, command})
  end

  @doc """
  Writes a value to the storage.
  """
  def write(storage, key, value) do
    GenServer.call(storage, {:write, key, value})
  end

  @doc """
  Applies a command to storage with position and label.
  """
  def apply(storage, command, position, label) do
    GenServer.call(storage, {:apply, command, position, label})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    parent_name = Keyword.get(opts, :name, __MODULE__)
    disk_name = String.to_atom("#{parent_name}_disk")
    disk_opts = Keyword.put(opts, :name, disk_name)
    disk = Eliraft.Storage.Disk.new(disk_opts)
    
    # Load persisted state if available
    state_machine = case Disk.read(disk, :state_machine) do
      {:ok, sm} when is_map(sm) -> sm
      _ -> %{}
    end
    
    metadata = case Disk.read(disk, :metadata) do
      {:ok, md} when is_map(md) -> md
      _ -> %{}
    end
    
    state = %__MODULE__{
      name: Keyword.get(opts, :name),
      table: Keyword.get(opts, :table),
      partition: Keyword.get(opts, :partition),
      provider: nil,
      handle: nil,
      position: 0,
      label: nil,
      config: %{},
      disk_storage: disk,
      state_machine: state_machine,
      metadata: metadata,
      disk: disk,
      state: :follower,
      current_term: 0
    }
    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, %{
      position: state.position,
      label: state.label,
      config: state.config,
      state: state.state,
      term: state.current_term
    }, state}
  end

  @impl true
  def handle_call(:position, _from, state) do
    {:reply, state.position, state}
  end

  @impl true
  def handle_call(:label, _from, state) do
    {:reply, state.label, state}
  end

  @impl true
  def handle_call(:config, _from, state) do
    {:reply, state.config, state}
  end

  @impl true
  def handle_call({:read, key}, _from, state) do
    case Map.get(state.state_machine, key) do
      nil -> {:reply, {:error, :not_found}, state}
      value -> {:reply, {:ok, value}, state}
    end
  end

  @impl true
  def handle_call({:apply, command}, _from, state) do
    case command do
      {:set, key, value} ->
        new_state_machine = Map.put(state.state_machine, key, value)
        new_state = %{state | state_machine: new_state_machine}
        persist_state(new_state)
        {:reply, :ok, new_state}
      _ ->
        {:reply, {:error, :invalid_command}, state}
    end
  end

  @impl true
  def handle_call({:apply, command, position, label}, _from, state) do
    if position < state.position do
      {:reply, {:error, :invalid_position}, state}
    else
      case command do
        {:set, key, value} ->
          new_state_machine = Map.put(state.state_machine, key, value)
          new_state = %{state | 
            state_machine: new_state_machine,
            position: position,
            label: label
          }
          persist_state(new_state)
          {:reply, :ok, new_state}
        _ ->
          {:reply, {:error, :invalid_command}, state}
      end
    end
  end

  @impl true
  def handle_call({:write, key, value}, _from, state) do
    new_state = %{state | metadata: Map.put(state.metadata, key, value)}
    persist_state(new_state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get, _from, state) do
    {:reply, Eliraft.Storage.Disk.get(state.disk), state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    new_disk = Eliraft.Storage.Disk.put(state.disk, key, value)
    {:reply, :ok, %{state | disk: new_disk}}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    new_disk = Eliraft.Storage.Disk.delete(state.disk, key)
    {:reply, :ok, %{state | disk: new_disk}}
  end

  @impl true
  def handle_call(:clear, _from, state) do
    new_disk = Eliraft.Storage.Disk.clear(state.disk)
    {:reply, :ok, %{state | disk: new_disk}}
  end

  @impl true
  def handle_call({:set_state, new_state}, _from, state) do
    {:reply, :ok, %{state | state: new_state}}
  end

  @impl true
  def handle_call({:set_term, new_term}, _from, state) do
    {:reply, :ok, %{state | current_term: new_term}}
  end

  @impl true
  def handle_call(request, _from, state) do
    Logger.warning("Unhandled call to Storage: #{inspect(request)}")
    {:reply, {:error, :unhandled_call}, state}
  end

  # Private Functions

  defp persist_state(state) do
    Disk.write(state.disk_storage, :state_machine, state.state_machine)
    Disk.write(state.disk_storage, :metadata, state.metadata)
  end
end 