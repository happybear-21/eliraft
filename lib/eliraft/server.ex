defmodule Eliraft.Server do
  @moduledoc """
  The Raft server implementation.
  """

  use GenServer
  require Logger

  alias Eliraft.{Identity, Config, Log, Storage, Storage.Disk, Server}

  @election_timeout_min 150
  @election_timeout_max 300
  @heartbeat_interval 50

  defstruct [
    :name,
    :log,
    :role,
    :current_term,
    :voted_for,
    :commit_index,
    :last_applied,
    :next_index,
    :match_index,
    :membership,
    :leader,
    :election_timer,
    :heartbeat_timer,
    :storage
  ]

  defmodule AppendEntriesRequest do
    defstruct [
      :term,
      :leader_id,
      :prev_log_index,
      :prev_log_term,
      :entries,
      :leader_commit
    ]
  end

  defmodule AppendEntriesResponse do
    defstruct [
      :term,
      :success
    ]
  end

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    state = %{
      name: Keyword.get(opts, :name, __MODULE__),
      role: :follower,
      current_term: 0,
      voted_for: nil,
      log: Keyword.get(opts, :log),
      storage: Keyword.get(opts, :storage),
      commit_index: 0,
      last_applied: 0,
      next_index: %{},
      match_index: %{},
      membership: %{},
      election_timer: nil,
      heartbeat_timer: nil,
      leader: nil
    }
    state = schedule_election_timeout(state)
    {:ok, state}
  end

  def get_state(server) do
    GenServer.call(server, :get_state)
  end

  def get_term(server) do
    GenServer.call(server, :get_term)
  end

  def append(server, entry) do
    GenServer.call(server, {:append, entry})
  end

  def read(server, key) do
    GenServer.call(server, {:read, key})
  end

  def get_commit_index(server) do
    GenServer.call(server, :get_commit_index)
  end

  def membership(server) do
    GenServer.call(server, :membership)
  end

  @impl true
  def handle_info(:election_timeout, state) do
    if state.role == :follower do
      new_state = %{state |
        role: :candidate,
        current_term: state.current_term + 1,
        voted_for: self(),
        leader: nil
      }
      new_state = schedule_election_timeout(new_state)
      {:noreply, new_state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state.role, state}
  end

  @impl true
  def handle_call(:get_term, _from, state) do
    {:reply, state.current_term, state}
  end

  @impl true
  def handle_call(:get_commit_index, _from, state) do
    {:reply, state.commit_index, state}
  end

  @impl true
  def handle_call(:membership, _from, state) do
    {:reply, state.membership, state}
  end

  def handle_call({:append, %{term: term, command: command}}, _from, state) do
    if state.role == :leader do
      case Log.append(state.log, %{term: term, command: command}) do
        :ok ->
          new_state = %{state | commit_index: state.commit_index + 1}
          replicate_to_followers(new_state)
          {:reply, :ok, new_state}

        error ->
          {:reply, error, state}
      end
    else
      {:reply, {:error, :not_leader}, state}
    end
  end

  def handle_call({:read, key}, _from, state) do
    if state.role == :leader do
      case Log.get_entries(state.log, 0, state.commit_index) do
        {:ok, entries} ->
          value = find_value_in_entries(entries, key)
          {:reply, {:ok, value}, state}

        error ->
          {:reply, error, state}
      end
    else
      {:reply, {:error, :not_leader}, state}
    end
  end

  def handle_call(
        {:request_vote,
         %{
           term: term,
           candidate_id: candidate_id,
           last_log_index: last_log_index,
           last_log_term: last_log_term
         }},
        _from,
        state
      ) do
    cond do
      term < state.current_term ->
        {:reply, {:error, :stale_term}, state}

      term > state.current_term ->
        new_state = %{state | 
          current_term: term, 
          voted_for: nil, 
          role: :follower,
          leader: nil
        }
        new_state = schedule_election_timeout(new_state)
        {:reply, {:error, :stale_term}, new_state}

      state.voted_for != nil and state.voted_for != candidate_id ->
        {:reply, {:error, :already_voted}, state}

      true ->
        case Log.get_entries(state.log, 0, last_log_index) do
          {:ok, entries} ->
            if is_log_consistent?(entries, last_log_term) do
              new_state = %{state | 
                voted_for: candidate_id,
                role: :follower,
                leader: candidate_id
              }
              new_state = schedule_election_timeout(new_state)
              {:reply, :ok, new_state}
            else
              {:reply, {:error, :inconsistent_log}, state}
            end

          error ->
            {:reply, error, state}
        end
    end
  end

  def handle_call({:append_entries, %{term: term, entries: entries, leader_commit: leader_commit}}, _from, state) do
    cond do
      term < state.current_term ->
        {:reply, {:error, :stale_term}, state}
      
      true ->
        case Log.append_many(state.log, entries) do
          :ok ->
            new_commit_index = min(leader_commit, state.commit_index + length(entries))
            new_state = %{state | 
              commit_index: new_commit_index,
              current_term: term,
              role: :follower,
              leader: self()
            }
            new_state = schedule_election_timeout(new_state)
            {:reply, :ok, new_state}

          error ->
            {:reply, error, state}
        end
    end
  end

  def handle_call({:replicate_log, follower_log}, _from, state) do
    case Log.get_entries(state.log, 0, state.commit_index) do
      {:ok, entries} ->
        case Log.append_many(follower_log, entries) do
          :ok ->
            {:reply, :ok, state}

          error ->
            {:reply, error, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:set_state, new_role}, _from, state) do
    {:reply, :ok, %{state | role: new_role}}
  end

  def handle_call({:set_term, new_term}, _from, state) do
    {:reply, :ok, %{state | current_term: new_term}}
  end

  def handle_vote(server, {:vote_granted, term}) do
    GenServer.call(server, {:vote_granted, term})
  end

  def handle_call({:vote_granted, term}, _from, state) do
    new_state =
      if term >= state.current_term do
        %{state | role: :leader, current_term: term}
      else
        state
      end

    {:reply, :ok, new_state}
  end

  def add_member(server, member) do
    GenServer.call(server, {:add_member, member})
  end

  def handle_call({:add_member, member}, _from, state) do
    new_membership = Map.put(state.membership, member, true)
    {:reply, :ok, %{state | membership: new_membership}}
  end

  def handle_vote_request(server, candidate, term) do
    GenServer.call(server, {:handle_vote_request, candidate, term})
  end

  def handle_call({:handle_vote_request, candidate, term}, _from, state) do
    cond do
      term < state.current_term ->
        {:reply, {:error, :stale_term}, state}
      term == state.current_term and state.voted_for != nil and state.voted_for != candidate ->
        {:reply, {:error, :already_voted}, state}
      true ->
        new_state = %{state |
          current_term: term,
          voted_for: candidate,
          role: :follower
        }
        {:reply, :ok, new_state}
    end
  end

  def become_leader(server) do
    GenServer.call(server, :become_leader)
  end

  def handle_call(:become_leader, _from, state) do
    {:reply, :ok, %{state | role: :leader}}
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, %{
      state: state.role,
      term: state.current_term,
      leader: state.voted_for,
      membership: state.membership
    }, state}
  end

  @impl true
  def handle_call({:handle_append_entries, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit}, _from, state) do
    cond do
      term < state.current_term ->
        {:reply, {:error, :stale_term}, state}
      prev_log_index > 0 and (prev_log_term != state.current_term or prev_log_index > state.commit_index) ->
        {:reply, {:error, :log_inconsistency}, state}
      true ->
        new_state = %{state |
          current_term: term,
          role: :follower,
          commit_index: min(leader_commit, state.commit_index + length(entries))
        }
        {:reply, :ok, new_state}
    end
  end

  def handle_call(request, _from, state) do
    Logger.warning("Unhandled call to Server: #{inspect(request)}")
    {:reply, {:error, :unhandled_call}, state}
  end

  defp start_election(state) do
    new_term = state.current_term + 1

    new_state = %{
      state
      | role: :candidate,
        current_term: new_term,
        voted_for: state.name,
        election_timer: nil
    }

    schedule_election_timeout(new_state)
    new_state
  end

  defp schedule_election_timeout(state) do
    if state.election_timer != nil do
      Process.cancel_timer(state.election_timer)
    end

    timeout = :rand.uniform(@election_timeout_max - @election_timeout_min) + @election_timeout_min
    timer = Process.send_after(self(), :election_timeout, timeout)
    %{state | election_timer: timer}
  end

  defp schedule_heartbeat(state) do
    timer = Process.send_after(self(), :heartbeat, @heartbeat_interval)
    %{state | heartbeat_timer: timer}
  end

  defp replicate_logs(state) do
    for {follower, _} <- state.membership do
      case Log.get_entries(state.log, 0, state.commit_index) do
        {:ok, entries} ->
          GenServer.cast(follower, {:append_entries, entries})

        _ ->
          :ok
      end
    end
  end

  defp is_log_consistent?(entries, last_log_term) do
    case List.last(entries) do
      nil -> true
      last_entry -> last_entry.term == last_log_term
    end
  end

  defp find_value_in_entries(entries, key) do
    entries
    |> Enum.reverse()
    |> Enum.find_value(fn entry ->
      case entry.command do
        {:set, ^key, value} -> value
        _ -> nil
      end
    end)
  end

  def handle_call(:get, _from, state) do
    {:reply, Eliraft.Storage.Disk.get(state.storage), state}
  end

  def handle_call({:put, key, value}, _from, state) do
    new_storage = Eliraft.Storage.Disk.put(state.storage, key, value)
    {:reply, :ok, %{state | storage: new_storage}}
  end

  def handle_call({:delete, key}, _from, state) do
    new_storage = Eliraft.Storage.Disk.delete(state.storage, key)
    {:reply, :ok, %{state | storage: new_storage}}
  end

  def handle_call(:clear, _from, state) do
    new_storage = Eliraft.Storage.Disk.clear(state.storage)
    {:reply, :ok, %{state | storage: new_storage}}
  end

  def set_state(server, new_state) do
    GenServer.call(server, {:set_state, new_state})
  end

  def set_term(server, new_term) do
    GenServer.call(server, {:set_term, new_term})
  end

  defp replicate_to_followers(state) do
    case Log.get_entries(state.log, state.commit_index, :infinity) do
      {:ok, entries} ->
        Enum.each(state.membership, fn {follower, _} ->
          GenServer.cast(follower, {:append_entries, %{
            term: state.current_term,
            entries: entries,
            leader_commit: state.commit_index
          }})
        end)
      _ -> :ok
    end
  end
end
