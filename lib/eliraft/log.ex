defmodule Eliraft.Log do
  @moduledoc """
  Handles the Raft log operations.
  """

  use GenServer
  require Logger

  # Types
  @type entry :: %{
    term: non_neg_integer(),
    command: term(),
    index: non_neg_integer()
  }

  # Server state
  defstruct [
    :entries,
    :current_index
  ]

  # Public API

  @doc """
  Starts a new log server.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @doc """
  Gets all log entries.
  """
  def get_entries(log) do
    GenServer.call(log, :get_entries)
  end

  @doc """
  Gets a specific log entry by index.
  """
  def get_entry(log, index) do
    GenServer.call(log, {:get_entry, index})
  end

  @doc """
  Appends a new entry to the log.
  """
  def append(log, entry) do
    GenServer.call(log, {:append, entry})
  end

  @doc """
  Appends multiple entries to the log.
  """
  def append_many(log, entries, _term) do
    GenServer.call(log, {:append_entries, entries})
  end

  @doc """
  Gets entries between start_index and end_index (for test compatibility).
  """
  def get_entries(log, start_index, end_index) do
    entries = get_entries(log)
    {:ok, Enum.slice(entries, start_index, end_index - start_index)}
  end

  @doc """
  Gets the term for a given index.
  """
  def get_term(log, index) do
    case get_entry(log, index) do
      {:ok, nil} -> {:ok, 0}
      {:ok, entry} -> {:ok, entry.term}
      {:error, _} -> {:ok, 0}
    end
  end

  @doc """
  Truncates the log at the given index.
  """
  def truncate(log, index) do
    GenServer.call(log, {:truncate, index})
  end

  # Server Callbacks

  @impl true
  def init(_opts) do
    state = %__MODULE__{
      entries: [],
      current_index: 0
    }
    {:ok, state}
  end

  @impl true
  def handle_call(:get_entries, _from, state) do
    {:reply, state.entries, state}
  end

  @impl true
  def handle_call({:get_entry, index}, _from, state) do
    case Enum.find(state.entries, fn entry -> entry.index == index end) do
      nil -> {:reply, {:ok, nil}, state}
      entry -> {:reply, {:ok, entry}, state}
    end
  end

  @impl true
  def handle_call({:append, entry}, _from, state) do
    new_index = state.current_index + 1
    new_entry = Map.put(entry, :index, new_index)
    new_entries = state.entries ++ [new_entry]
    new_state = %{state | entries: new_entries, current_index: new_index}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:append_entries, entries}, _from, state) do
    {new_entries, new_index} = Enum.reduce(entries, {state.entries, state.current_index}, fn entry, {acc_entries, acc_index} ->
      new_index = acc_index + 1
      # Remove any existing :index field and assign a new one
      entry = Map.delete(entry, :index)
      new_entry = Map.put(entry, :index, new_index)
      {acc_entries ++ [new_entry], new_index}
    end)
    new_state = %{state | entries: new_entries, current_index: new_index}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:truncate, index}, _from, state) do
    new_entries = Enum.filter(state.entries, fn entry -> entry.index <= index end)
    new_state = %{state | entries: new_entries, current_index: index}
    {:reply, :ok, new_state}
  end
end 