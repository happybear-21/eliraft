defmodule Eliraft.Log do
  @moduledoc """
  Implements the persistent log of commands applied to the state machine.
  """

  use GenServer

  # Types
  @type log :: %__MODULE__{
    table: atom(),
    partition: non_neg_integer(),
    view: term(),
    storage: pid()
  }

  defstruct [:table, :partition, :view, :storage]

  # Client API

  @doc """
  Starts a new log server.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
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
  def append_many(log, entries, term) do
    GenServer.call(log, {:append_many, entries, term})
  end

  @doc """
  Truncates the log at the given index.
  """
  def truncate(log, index) do
    GenServer.call(log, {:truncate, index})
  end

  @doc """
  Gets the term for a given index.
  """
  def get_term(log, index) do
    GenServer.call(log, {:get_term, index})
  end

  @doc """
  Gets an entry at the given index.
  """
  def get_entry(log, index) do
    GenServer.call(log, {:get_entry, index})
  end

  @doc """
  Gets entries between start_index and end_index.
  """
  def get_entries(log, start_index, end_index) do
    GenServer.call(log, {:get_entries, start_index, end_index})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    table = Keyword.get(opts, :table)
    partition = Keyword.get(opts, :partition)
    storage = Keyword.get(opts, :storage)

    state = %__MODULE__{
      table: table,
      partition: partition,
      view: nil,
      storage: storage
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:append, _entry}, _from, state) do
    # TODO: Implement append logic
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:append_many, _entries, _term}, _from, state) do
    # TODO: Implement append_many logic
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:truncate, _index}, _from, state) do
    # TODO: Implement truncate logic
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get_term, _index}, _from, state) do
    # TODO: Implement get_term logic
    {:reply, {:ok, 0}, state}
  end

  @impl true
  def handle_call({:get_entry, _index}, _from, state) do
    # TODO: Implement get_entry logic
    {:reply, {:ok, nil}, state}
  end

  @impl true
  def handle_call({:get_entries, _start_index, _end_index}, _from, state) do
    # TODO: Implement get_entries logic
    {:reply, {:ok, []}, state}
  end

  # Private Functions

  defp make_name(table, partition) do
    :"#{table}_#{partition}_log"
  end
end 