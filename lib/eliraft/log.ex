defmodule Eliraft.Log do
  @moduledoc """
  The Raft log implementation.
  """

  use GenServer
  require Logger

  alias Eliraft.{Storage, Storage.Disk, Server}

  # Types
  @type entry :: %{
          term: non_neg_integer(),
          command: term(),
          index: non_neg_integer()
        }

  # Server state
  defstruct [
    :entries,
    :current_term,
    :voted_for,
    :disk
  ]

  # Public API

  @doc """
  Starts a new log server.
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Appends an entry to the log.
  """
  def append(log, entry) do
    GenServer.call(log, {:append, entry})
  end

  @doc """
  Appends multiple entries to the log.
  """
  def append_many(log, entries) do
    GenServer.call(log, {:append_many, entries})
  end

  @doc """
  Gets an entry from the log.
  """
  def get_entry(log, index) do
    GenServer.call(log, {:get_entry, index})
  end

  @doc """
  Gets multiple entries from the log.
  """
  def get_entries(log, start_index, end_index) do
    GenServer.call(log, {:get_entries, start_index, end_index})
  end

  @doc """
  Gets the current term.
  """
  def get_term(log) do
    GenServer.call(log, :get_term)
  end

  @doc """
  Truncates the log at the given index.
  """
  def truncate(log, index) do
    GenServer.call(log, {:truncate, index})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    data_dir = Keyword.get(opts, :data_dir, "data")
    log_name = Keyword.get(opts, :name, __MODULE__)
    disk_name = String.to_atom("#{log_name}_disk")
    disk = Eliraft.Storage.Disk.new(name: disk_name)

    # Load persisted state if available
    entries = case Disk.read(disk, :entries) do
      {:ok, e} when is_list(e) -> e
      _ -> []
    end

    current_term = case Disk.read(disk, :current_term) do
      {:ok, t} when is_integer(t) -> t
      _ -> 0
    end

    voted_for = case Disk.read(disk, :voted_for) do
      {:ok, v} -> v
      _ -> nil
    end

    state = %__MODULE__{
      entries: entries,
      current_term: current_term,
      voted_for: voted_for,
      disk: disk
    }

    Logger.notice("Log[#{inspect(opts[:name])}] starting")
    {:ok, state}
  end

  @impl true
  def handle_call({:append, entry}, _from, state) do
    if entry.term >= state.current_term do
      new_entries = state.entries ++ [entry]
      new_state = %{state | entries: new_entries, current_term: entry.term}
      persist_state(new_state)
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :stale_term}, state}
    end
  end

  @impl true
  def handle_call({:append_many, entries}, _from, state) do
    if Enum.any?(entries, &(&1.term < state.current_term)) do
      {:reply, {:error, :stale_term}, state}
    else
      new_entries = state.entries ++ entries
      new_term = Enum.max([state.current_term | Enum.map(entries, & &1.term)])
      new_state = %{state | entries: new_entries, current_term: new_term}
      persist_state(new_state)
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:truncate, index}, _from, state) do
    if index >= 0 and index < length(state.entries) do
      new_entries = Enum.take(state.entries, index + 1)
      new_state = %{state | entries: new_entries}
      persist_state(new_state)
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :invalid_index}, state}
    end
  end

  @impl true
  def handle_call({:get_entries, start_index, end_index}, _from, state) do
    cond do
      is_integer(end_index) and start_index >= 0 and end_index >= start_index ->
        entries = Enum.slice(state.entries, start_index..(end_index - 1))
        {:reply, {:ok, entries}, state}
      end_index == :infinity and start_index >= 0 ->
        entries = Enum.slice(state.entries, start_index..-1)
        {:reply, {:ok, entries}, state}
      true ->
        {:reply, {:error, :invalid_index}, state}
    end
  end

  @impl true
  def handle_call({:get_entry, index}, _from, state) do
    if index >= 0 and index < length(state.entries) do
      entry = Enum.at(state.entries, index)
      {:reply, {:ok, entry}, state}
    else
      {:reply, {:ok, nil}, state}
    end
  end

  @impl true
  def handle_call(:get_term, _from, state) do
    {:reply, {:ok, state.current_term}, state}
  end

  @impl true
  def handle_call(request, _from, state) do
    Logger.warning("Unhandled call to Log: #{inspect(request)}")
    {:reply, {:error, :unhandled_call}, state}
  end

  @impl true
  def terminate(_reason, state) do
    persist_state(state)
    :ok
  end

  # Private Functions

  defp persist_state(state) do
    Disk.write(state.disk, :entries, state.entries)
    Disk.write(state.disk, :current_term, state.current_term)
    Disk.write(state.disk, :voted_for, state.voted_for)
  end
end
