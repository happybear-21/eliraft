defmodule Eliraft.Storage.Disk do
  @moduledoc """
  Provides disk-based storage for Raft log entries and state machine.
  """

  use GenServer
  require Logger

  @log_file "log.entries"
  @state_file "state.term"

  # Types
  @type entry :: %{
    term: non_neg_integer(),
    command: term(),
    index: non_neg_integer()
  }

  # Server state
  defstruct [:data_dir, :log_file, :state_file, entries: [], current_index: 0, current_term: 0]

  # Public API

  @doc """
  Starts a new disk storage server.
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Appends an entry to the log file.
  """
  def append(server, entry) do
    GenServer.call(server, {:append, entry})
  end

  @doc """
  Reads entries from the log file.
  """
  def read(server, key) do
    GenServer.call(server, {:read, key})
  end

  @doc """
  Truncates the log file at the given index.
  """
  def truncate(server, index) do
    GenServer.call(server, {:truncate, index})
  end

  @doc """
  Saves the current state to disk.
  """
  def save_state(server, state) do
    GenServer.call(server, {:save_state, state})
  end

  @doc """
  Loads the state from disk.
  """
  def load_state(server) do
    GenServer.call(server, :load_state)
  end

  @doc """
  Writes a key-value pair to the storage.
  """
  def write(server, key, value) do
    GenServer.call(server, {:write, key, value})
  end

  @doc """
  Writes entries to the log file.
  """
  def write(server, :entries, entries) do
    GenServer.call(server, {:write, :entries, entries})
  end

  @doc """
  Writes the state machine to disk.
  """
  def write(server, :state_machine, state_machine) do
    GenServer.call(server, {:write, :state_machine, state_machine})
  end

  @doc """
  Writes metadata to disk.
  """
  def write(server, :metadata, metadata) do
    GenServer.call(server, {:write, :metadata, metadata})
  end

  @doc """
  Writes the current index to disk.
  """
  def write(server, :current_index, current_index) do
    GenServer.call(server, {:write, :current_index, current_index})
  end

  @doc """
  Writes the current term to disk.
  """
  def write(server, :current_term, current_term) do
    GenServer.call(server, {:write, :current_term, current_term})
  end

  @doc """
  Reads entries from the log file.
  """
  def read(server, :entries) do
    GenServer.call(server, {:read, :entries})
  end

  @doc """
  Reads the state machine from disk.
  """
  def read(server, :state_machine) do
    GenServer.call(server, {:read, :state_machine})
  end

  @doc """
  Reads metadata from disk.
  """
  def read(server, :metadata) do
    GenServer.call(server, {:read, :metadata})
  end

  @doc """
  Reads the current index from disk.
  """
  def read(server, :current_index) do
    GenServer.call(server, {:read, :current_index})
  end

  @doc """
  Reads the current term from disk.
  """
  def read(server, :current_term) do
    GenServer.call(server, {:read, :current_term})
  end

  def new(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    data_dir = Keyword.get(opts, :data_dir, "data")
    case start_link([name: name, data_dir: data_dir]) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
      other ->
        raise "Failed to start or get disk process: #{inspect(other)}"
    end
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name)
    table = Keyword.get(opts, :table)
    partition = Keyword.get(opts, :partition)
    data_dir = Keyword.get(opts, :data_dir, "data")
    File.mkdir_p!(data_dir)

    state = %{
      name: name,
      table: table,
      partition: partition,
      data_dir: data_dir,
      log_file: data_dir <> "/log.entries",
      current_index: 0,
      current_term: 0,
      entries: []
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:append, entry}, _from, state) do
    # Add index to entry
    entry = Map.put(entry, :index, state.current_index)
    
    # Append to entries list
    new_entries = state.entries ++ [entry]
    
    # Write to disk
    :ok = write_log(state.log_file, new_entries)
    
    # Update state
    new_state = %{state | 
      entries: new_entries,
      current_index: state.current_index + 1
    }
    
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:read, index}, _from, state) do
    entry = Enum.find(state.entries, &(&1.index == index))
    {:reply, {:ok, entry}, state}
  end

  @impl true
  def handle_call({:truncate, index}, _from, state) do
    # Keep entries up to and including the given index
    new_entries = Enum.filter(state.entries, &(&1.index <= index))
    
    # Write truncated log to disk
    :ok = write_log(state.log_file, new_entries)
    
    # Update state
    new_state = %{state | 
      entries: new_entries,
      current_index: length(new_entries)
    }
    
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:save_state, state}, _from, disk_state) do
    state_data = :erlang.term_to_binary(state)
    :ok = File.write(disk_state.state_file, state_data)
    {:reply, :ok, disk_state}
  end

  @impl true
  def handle_call(:load_state, _from, state) do
    case File.read(state.state_file) do
      {:ok, data} ->
        try do
          loaded_state = :erlang.binary_to_term(data)
          {:reply, {:ok, loaded_state}, state}
        rescue
          ArgumentError ->
            {:reply, {:ok, nil}, state}
        end
      {:error, :enoent} ->
        {:reply, {:ok, nil}, state}
    end
  end

  @impl true
  def handle_call({:write, key, value}, _from, state) do
    # Write to state machine file
    state_machine_file = Path.join(state.data_dir, "state_machine.bin")
    current_state_machine = case File.read(state_machine_file) do
      {:ok, data} ->
        :erlang.binary_to_term(data)
      _ ->
        %{}
    end
    
    new_state_machine = Map.put(current_state_machine, key, value)
    :ok = File.write(state_machine_file, :erlang.term_to_binary(new_state_machine))
    
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:read, key}, _from, state) do
    state_machine_file = Path.join(state.data_dir, "state_machine.bin")
    case File.read(state_machine_file) do
      {:ok, data} ->
        current_state_machine = :erlang.binary_to_term(data)
        case Map.get(current_state_machine, key) do
          nil -> {:reply, {:error, :not_found}, state}
          value -> {:reply, {:ok, value}, state}
        end
      _ ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:write, :entries, entries}, _from, state) do
    # Write entries to log file
    :ok = write_log(state.log_file, entries)
    
    # Update state
    new_state = %{state | 
      entries: entries,
      current_index: length(entries)
    }
    
    # Write current index to disk
    index_file = Path.join(state.data_dir, "current_index.bin")
    :ok = File.write(index_file, :erlang.term_to_binary(new_state.current_index))
    
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:write, :state_machine, state_machine}, _from, state) do
    :ok = File.write(state.data_dir <> "/state_machine.bin", :erlang.term_to_binary(state_machine))
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:write, :metadata, metadata}, _from, state) do
    :ok = File.write(state.data_dir <> "/metadata.bin", :erlang.term_to_binary(metadata))
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:write, :current_index, current_index}, _from, state) do
    :ok = File.write(state.data_dir <> "/current_index.bin", :erlang.term_to_binary(current_index))
    new_state = %{state | current_index: current_index}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:write, :current_term, current_term}, _from, state) do
    :ok = File.write(state.data_dir <> "/current_term.bin", :erlang.term_to_binary(current_term))
    new_state = Map.put(state, :current_term, current_term)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:read, :entries}, _from, state) do
    entries =
      case File.read(state.log_file) do
        {:ok, data} ->
          try do
            :erlang.binary_to_term(data)
          rescue
            _ -> []
          end
        _ -> []
      end
    {:reply, {:ok, entries}, state}
  end

  @impl true
  def handle_call({:read, :state_machine}, _from, state) do
    value =
      case File.read(state.data_dir <> "/state_machine.bin") do
        {:ok, data} ->
          try do
            :erlang.binary_to_term(data)
          rescue
            _ -> %{}
          end
        _ -> %{}
      end
    {:reply, {:ok, value}, state}
  end

  @impl true
  def handle_call({:read, :metadata}, _from, state) do
    value =
      case File.read(state.data_dir <> "/metadata.bin") do
        {:ok, data} ->
          try do
            :erlang.binary_to_term(data)
          rescue
            _ -> %{label: nil, position: 0}
          end
        _ -> %{label: nil, position: 0}
      end
    {:reply, {:ok, value}, state}
  end

  @impl true
  def handle_call({:read, :current_index}, _from, state) do
    value =
      case File.read(state.data_dir <> "/current_index.bin") do
        {:ok, data} ->
          try do
            :erlang.binary_to_term(data)
          rescue
            _ -> 0
          end
        _ -> 0
      end
    {:reply, {:ok, value}, state}
  end

  @impl true
  def handle_call({:read, :current_term}, _from, state) do
    value =
      case File.read(state.data_dir <> "/current_term.bin") do
        {:ok, data} ->
          try do
            :erlang.binary_to_term(data)
          rescue
            _ -> 0
          end
        _ -> 0
      end
    {:reply, {:ok, value}, state}
  end

  @impl true
  def handle_call({:read, :voted_for}, _from, state) do
    value =
      case File.read(state.data_dir <> "/voted_for.bin") do
        {:ok, data} ->
          try do
            :erlang.binary_to_term(data)
          rescue
            _ -> nil
          end
        _ -> nil
      end
    {:reply, {:ok, value}, state}
  end

  def get(server) do
    GenServer.call(server, :get)
  end

  def put(server, key, value) do
    GenServer.call(server, {:put, key, value})
  end

  def delete(server, key) do
    GenServer.call(server, {:delete, key})
  end

  def clear(server) do
    GenServer.call(server, :clear)
  end

  def handle_call(:get, _from, state) do
    {:reply, %{}, state}
  end

  def handle_call({:put, key, value}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:delete, key}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call(:clear, _from, state) do
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Flush state to disk
    write_log(state.log_file, state.entries)
    :ok = File.write(Path.join(state.data_dir, "current_index.bin"), :erlang.term_to_binary(state.current_index))
    :ok = File.write(Path.join(state.data_dir, "current_term.bin"), :erlang.term_to_binary(state.current_term))
    :ok
  end

  # Private Functions

  defp write_log(log_file, entries) do
    # Write entries to temporary file first
    temp_file = log_file <> ".tmp"
    :ok = File.write(temp_file, :erlang.term_to_binary(entries))
    
    # Atomically rename temp file to log file
    File.rename(temp_file, log_file)
  end
end 