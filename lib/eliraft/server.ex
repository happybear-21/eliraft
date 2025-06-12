defmodule Eliraft.Server do
  @moduledoc """
  Implements the Raft consensus protocol server.
  This is a port of the wa_raft_server Erlang implementation.
  """

  use GenServer
  require Logger

  alias Eliraft.{Identity, Config}

  # Constants
  @election_timeout_min 150
  @election_timeout_max 300
  @heartbeat_interval 50
  @commit_batch_interval 10

  # Types
  @type state :: :follower | :candidate | :leader | :disabled | :witness
  @type config :: %Config{}
  @type membership :: %{node() => boolean()}
  @type status :: %{
    state: state(),
    term: non_neg_integer(),
    leader: node() | nil,
    membership: membership()
  }

  # Server state
  defstruct [
    :name,
    :table,
    :partition,
    :current_term,
    :voted_for,
    :log,
    :commit_index,
    :last_applied,
    :next_index,
    :match_index,
    :leader_id,
    :config,
    :application,
    :state
  ]

  # Client API

  @doc """
  Starts a new Raft server.
  """
  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @doc """
  Returns the current status of the Raft server.
  """
  def status(server) do
    GenServer.call(server, :status)
  end

  @doc """
  Returns the current membership configuration.
  """
  def membership(server) do
    GenServer.call(server, :membership)
  end

  # Server Callbacks

  @impl true
  def init(args) do
    args = if is_map(args), do: args, else: Enum.into(args, %{})
    state = %__MODULE__{
      name: make_name(args.table, args.partition),
      table: args.table,
      partition: args.partition,
      current_term: 0,
      voted_for: nil,
      log: [],
      commit_index: 0,
      last_applied: 0,
      next_index: %{},
      match_index: %{},
      leader_id: nil,
      config: Config.new(),
      application: args.application,
      state: :follower
    }

    schedule_election_timeout()
    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      state: state.state,
      term: state.current_term,
      leader: state.leader_id,
      membership: state.config.members
    }
    {:reply, status, state}
  end

  @impl true
  def handle_call(:membership, _from, state) do
    {:reply, state.config.members, state}
  end

  @impl true
  def handle_info(:election_timeout, state) do
    case state.state do
      :follower ->
        new_state = %{state | state: :candidate, current_term: state.current_term + 1, voted_for: Node.self()}
        schedule_election_timeout()
        {:noreply, new_state}
      :candidate ->
        new_state = %{state | current_term: state.current_term + 1}
        schedule_election_timeout()
        {:noreply, new_state}
      _ ->
        {:noreply, state}
    end
  end

  # Private Functions

  defp make_name(table, partition) do
    :"#{table}_#{partition}"
  end

  defp schedule_election_timeout do
    timeout = :rand.uniform(@election_timeout_max - @election_timeout_min) + @election_timeout_min
    Process.send_after(self(), :election_timeout, timeout)
  end
end 