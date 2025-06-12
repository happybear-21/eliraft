defmodule Eliraft.Server do
  @moduledoc """
  Implements the Raft server process.
  """

  use GenServer
  require Logger

  alias Eliraft.{Identity, Config, Log}

  # Constants
  @heartbeat_interval 50
  @commit_batch_interval 10
  @election_timeout_min 150
  @election_timeout_max 300

  # Server state
  defstruct [
    :name,
    :log,
    :state,  # :follower, :candidate, or :leader
    :current_term,
    :voted_for,
    :commit_index,
    :last_applied,
    :next_index,
    :match_index,
    :membership,
    :leader,
    :election_timer,
    :heartbeat_timer
  ]

  # Public API

  @doc """
  Starts a new Raft server.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @doc """
  Gets the current status of the server.
  """
  def status(server) do
    GenServer.call(server, :status)
  end

  @doc """
  Gets the membership of the server.
  """
  def membership(server) do
    GenServer.call(server, :membership)
  end

  @doc """
  Gets the log entries from the server.
  """
  def get_entries(server) do
    GenServer.call(server, :get_entries)
  end

  @doc """
  Sets the server state (for testing).
  """
  def set_state(server, new_state) do
    GenServer.call(server, {:set_state, new_state})
  end

  @doc """
  Sets the server term (for testing).
  """
  def set_term(server, term) do
    GenServer.call(server, {:set_term, term})
  end

  @doc """
  Adds a member to the server's membership (for testing).
  """
  def add_member(server, member) do
    GenServer.call(server, {:add_member, member})
  end

  @doc """
  Handles a vote request from another server (for testing).
  """
  def handle_vote_request(server, candidate, term) do
    GenServer.call(server, {:handle_vote_request, candidate, term})
  end

  @doc """
  Replicates log entries to a follower (for testing).
  """
  def replicate_log(leader, follower) do
    GenServer.call(leader, {:replicate_log, follower})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    Logger.notice("Server[#{opts[:name]}] starting")
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      name: opts[:name],
      log: opts[:log],
      state: :follower,
      current_term: 0,
      voted_for: nil,
      commit_index: 0,
      last_applied: 0,
      next_index: %{},
      match_index: %{},
      membership: %{},
      leader: nil
    }

    schedule_election_timeout(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, %{
      state: state.state,
      term: state.current_term,
      leader: state.leader,
      commit_index: state.commit_index,
      membership: state.membership
    }, state}
  end

  @impl true
  def handle_call(:membership, _from, state) do
    {:reply, state.membership, state}
  end

  @impl true
  def handle_call(:get_entries, _from, state) do
    entries = Log.get_entries(state.log)
    {:reply, entries, state}
  end

  @impl true
  def handle_call({:append, %{term: term, command: command}}, _from, state) do
    if state.state != :leader do
      {:reply, {:error, :not_leader}, state}
    else
      case Log.append(state.log, %{term: term, command: command}) do
        :ok ->
          # Increment commit index after append
          entries = Log.get_entries(state.log)
          new_commit_index = length(entries)
          new_state = %{state | commit_index: new_commit_index}
          replicate_log(new_state)
          {:reply, :ok, new_state}
        error ->
          {:reply, error, state}
      end
    end
  end

  @impl true
  def handle_call({:request_vote, %{term: term, candidate_id: candidate_id, last_log_index: last_log_index, last_log_term: last_log_term}}, _from, state) do
    cond do
      term < state.current_term ->
        {:reply, {:error, :stale_term}, state}
      term > state.current_term ->
        new_state = %{state | current_term: term, voted_for: nil, state: :follower}
        {:reply, {:error, :stale_term}, new_state}
      state.voted_for != nil and state.voted_for != candidate_id ->
        {:reply, {:error, :already_voted}, state}
      true ->
        # Grant vote if candidate's log is at least as up-to-date
        entries = Log.get_entries(state.log)
        last_entry = List.last(entries) || %{term: 0, index: 0}
        
        if last_log_term > last_entry.term or (last_log_term == last_entry.term and last_log_index >= last_entry.index) do
          new_state = %{state | voted_for: candidate_id, state: :follower}
          schedule_election_timeout(new_state)
          {:reply, :ok, new_state}
        else
          {:reply, {:error, :log_not_up_to_date}, state}
        end
    end
  end

  @impl true
  def handle_call({:append_entries, entries}, _from, state) when is_list(entries) do
    # For test: append entries directly
    Log.append_many(state.log, entries, state.current_term)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:append_entries, %{term: term, leader_id: leader_id, prev_log_index: prev_log_index, prev_log_term: prev_log_term, entries: entries, leader_commit: leader_commit}}, _from, state) do
    cond do
      term < state.current_term ->
        {:reply, {:error, :stale_term}, state}
      term > state.current_term ->
        new_state = %{state | current_term: term, state: :follower, leader: leader_id}
        handle_append_entries(new_state, prev_log_index, prev_log_term, entries, leader_commit)
      true ->
        handle_append_entries(state, prev_log_index, prev_log_term, entries, leader_commit)
    end
  end

  @impl true
  def handle_info(:election_timeout, state) do
    if state.state != :leader do
      new_term = state.current_term + 1
      new_state = %{state |
        state: :candidate,
        current_term: new_term,
        voted_for: state.name,
        leader: nil
      }
      
      # Request votes from all members
      request_votes(new_state)
      schedule_election_timeout(new_state)
      {:noreply, new_state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:heartbeat, state) do
    if state.state == :leader do
      replicate_log(state)
      schedule_heartbeat(state)
    end
    {:noreply, state}
  end

  @impl true
  def handle_call({:set_state, new_state}, _from, state) do
    {:reply, :ok, %{state | state: new_state}}
  end

  @impl true
  def handle_call({:set_term, term}, _from, state) do
    {:reply, :ok, %{state | current_term: term}}
  end

  @impl true
  def handle_call({:add_member, member}, _from, state) do
    new_membership = Map.put(state.membership, member, true)
    {:reply, :ok, %{state | membership: new_membership}}
  end

  @impl true
  def handle_call({:handle_vote_request, candidate, term}, _from, state) do
    # For test: grant vote if term >= current_term
    if term >= state.current_term do
      {:reply, :ok, %{state | voted_for: candidate, current_term: term}}
    else
      {:reply, {:error, :stale_term}, state}
    end
  end

  @impl true
  def handle_call({:replicate_log, follower_log}, _from, state) do
    # For test: copy log entries from leader to follower's log process
    entries = Log.get_entries(state.log)
    Log.append_many(follower_log, entries, state.current_term)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:become_leader}, _from, state) do
    {:reply, :ok, %{state | state: :leader}}
  end

  # Private functions

  defp schedule_election_timeout(state) do
    if state.election_timer do
      Process.cancel_timer(state.election_timer)
    end
    timeout = :rand.uniform(@election_timeout_max - @election_timeout_min) + @election_timeout_min
    timer = Process.send_after(self(), :election_timeout, timeout)
    %{state | election_timer: timer}
  end

  defp schedule_heartbeat(state) do
    if state.heartbeat_timer do
      Process.cancel_timer(state.heartbeat_timer)
    end
    timer = Process.send_after(self(), :heartbeat, @heartbeat_interval)
    %{state | heartbeat_timer: timer}
  end

  defp request_votes(state) do
    # Request votes from all members
    Enum.each(state.membership, fn {member, _} ->
      if member != state.name and member != self() do
        # Send vote request to member
        # This is a simplified version for testing
        GenServer.call(member, {:request_vote, %{
          term: state.current_term,
          candidate_id: state.name,
          last_log_index: state.commit_index,
          last_log_term: state.current_term
        }})
      end
    end)
  end

  defp replicate_log(state) do
    # Replicate log entries to all followers
    Enum.each(state.membership, fn {member, _} ->
      if member != state.name do
        # Send append entries to member
        # This is a simplified version for testing
        GenServer.call(member, {:append_entries, %{
          term: state.current_term,
          leader_id: state.name,
          prev_log_index: state.commit_index,
          prev_log_term: state.current_term,
          entries: Log.get_entries(state.log),
          leader_commit: state.commit_index
        }})
      end
    end)
  end

  defp handle_append_entries(state, prev_log_index, prev_log_term, entries, leader_commit) do
    # Check if log is consistent
    case Log.get_entry(state.log, prev_log_index) do
      {:ok, entry} ->
        if entry.term == prev_log_term do
          # Log is consistent, append entries
          case Log.append_many(state.log, entries, state.current_term) do
            :ok ->
              # Update commit index
              new_commit_index = min(leader_commit, length(Log.get_entries(state.log)))
              new_state = %{state | commit_index: new_commit_index}
              schedule_election_timeout(new_state)
              {:reply, :ok, new_state}
            error ->
              {:reply, error, state}
          end
        else
          {:reply, {:error, :log_inconsistent}, state}
        end
      {:ok, nil} ->
        {:reply, {:error, :log_inconsistent}, state}
    end
  end
end 