defmodule Eliraft.Queue do
  @moduledoc """
  Manages request queues for Raft operations.
  This module implements tracking of pending requests and queue limits.
  """

  use GenServer
  require Logger

  # Constants
  @queue_table_options [:named_table, :public, {:read_concurrency, true}, {:write_concurrency, true}]
  @number_of_queue_size_counters 4
  @apply_queue_size_counter 1
  @apply_queue_byte_size_counter 2
  @commit_queue_size_counter 3
  @read_queue_size_counter 4

  # Types
  @type queues :: %__MODULE__{
    application: atom(),
    counters: :atomics.atomics_ref(),
    commits: atom(),
    reads: atom()
  }

  defstruct [
    :application,
    :counters,
    :commits,
    :reads
  ]

  # Public API

  @doc """
  Starts a new queue server.
  """
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args.queue_name)
  end

  @doc """
  Creates a new queue structure from options.
  """
  def queues(options) do
    %__MODULE__{
      application: options.application,
      counters: options.queue_counters,
      commits: options.queue_commits,
      reads: options.queue_reads
    }
  end

  @doc """
  Creates a new queue structure for a table and partition.
  """
  def queues(_table, _partition) do
    # TODO: Implement queue lookup from partition supervisor
    nil
  end

  @doc """
  Returns the size of the commit queue.
  """
  def commit_queue_size(%__MODULE__{counters: counters}) do
    :atomics.get(counters, @commit_queue_size_counter)
  end

  @doc """
  Returns the size of the commit queue for a table and partition.
  """
  def commit_queue_size(table, partition) do
    case queues(table, partition) do
      nil -> 0
      queue -> commit_queue_size(queue)
    end
  end

  @doc """
  Returns true if the commit queue is full.
  """
  def commit_queue_full(%__MODULE__{application: app, counters: counters}) do
    :atomics.get(counters, @commit_queue_size_counter) >= max_pending_commits(app)
  end

  @doc """
  Returns true if the commit queue is full for a table and partition.
  """
  def commit_queue_full(table, partition) do
    case queues(table, partition) do
      nil -> false
      queue -> commit_queue_full(queue)
    end
  end

  @doc """
  Returns the size of the apply queue.
  """
  def apply_queue_size(%__MODULE__{counters: counters}) do
    :atomics.get(counters, @apply_queue_size_counter)
  end

  @doc """
  Returns the size of the apply queue for a table and partition.
  """
  def apply_queue_size(table, partition) do
    case queues(table, partition) do
      nil -> 0
      queue -> apply_queue_size(queue)
    end
  end

  @doc """
  Returns the byte size of the apply queue.
  """
  def apply_queue_byte_size(%__MODULE__{counters: counters}) do
    :atomics.get(counters, @apply_queue_byte_size_counter)
  end

  @doc """
  Returns the byte size of the apply queue for a table and partition.
  """
  def apply_queue_byte_size(table, partition) do
    case queues(table, partition) do
      nil -> 0
      queue -> apply_queue_byte_size(queue)
    end
  end

  @doc """
  Returns true if the apply queue is full.
  """
  def apply_queue_full(%__MODULE__{application: app, counters: counters}) do
    :atomics.get(counters, @apply_queue_size_counter) >= max_pending_applies(app) or
      :atomics.get(counters, @apply_queue_byte_size_counter) >= max_pending_apply_bytes(app)
  end

  @doc """
  Returns true if the apply queue is full for a table and partition.
  """
  def apply_queue_full(table, partition) do
    case queues(table, partition) do
      nil -> false
      queue -> apply_queue_full(queue)
    end
  end

  @doc """
  Returns a new default queue struct.
  """
  def new() do
    %__MODULE__{
      application: nil,
      counters: nil,
      commits: nil,
      reads: nil
    }
  end

  # Server Callbacks

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)

    Logger.notice("Queue[#{args.queue_name}] starting for partition #{args.table}/#{args.partition}")

    # Create ETS tables for commits and reads
    commits = :ets.new(args.queue_commits, @queue_table_options)
    reads = :ets.new(args.queue_reads, @queue_table_options)

    # Create counters for queue sizes
    counters = :atomics.new(@number_of_queue_size_counters, [:atomics])

    state = %__MODULE__{
      application: args.application,
      counters: counters,
      commits: commits,
      reads: reads
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      commit_queue_size: commit_queue_size(state),
      apply_queue_size: apply_queue_size(state),
      apply_queue_byte_size: apply_queue_byte_size(state)
    }
    {:reply, status, state}
  end

  @impl true
  def handle_call({:commit, _key, _op}, _from, state) do
    # TODO: Implement commit logic
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:read, _command}, _from, state) do
    # TODO: Implement read logic
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:apply, _command}, _from, state) do
    # TODO: Implement apply logic
    {:reply, :ok, state}
  end

  # Private Functions

  defp max_pending_commits(_app) do
    # TODO: Get from application config
    1000
  end

  defp max_pending_applies(_app) do
    # TODO: Get from application config
    1000
  end

  defp max_pending_apply_bytes(_app) do
    # TODO: Get from application config
    1_000_000
  end
end 