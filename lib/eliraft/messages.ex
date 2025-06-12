defmodule Eliraft.Messages do
  @moduledoc """
  Defines the message types and structures used in the Raft protocol.
  """

  # Message types
  defmodule RequestVote do
    @moduledoc "RequestVote RPC message"
    defstruct [:term, :candidate_id, :last_log_index, :last_log_term]
  end

  defmodule RequestVoteResponse do
    @moduledoc "RequestVote RPC response"
    defstruct [:term, :vote_granted]
  end

  defmodule AppendEntries do
    @moduledoc "AppendEntries RPC message"
    defstruct [:term, :leader_id, :prev_log_index, :prev_log_term, :entries, :leader_commit]
  end

  defmodule AppendEntriesResponse do
    @moduledoc "AppendEntries RPC response"
    defstruct [:term, :success]
  end

  # Message handling functions
  def handle_request_vote(state, %RequestVote{} = request) do
    cond do
      request.term < state.current_term ->
        %RequestVoteResponse{term: state.current_term, vote_granted: false}
      
      request.term > state.current_term ->
        # TODO: Implement term update and voting logic
        %RequestVoteResponse{term: request.term, vote_granted: false}
      
      true ->
        # TODO: Implement voting logic for same term
        %RequestVoteResponse{term: state.current_term, vote_granted: false}
    end
  end

  def handle_append_entries(state, %AppendEntries{} = request) do
    cond do
      request.term < state.current_term ->
        %AppendEntriesResponse{term: state.current_term, success: false}
      
      request.term > state.current_term ->
        # TODO: Implement term update and log replication
        %AppendEntriesResponse{term: request.term, success: false}
      
      true ->
        # TODO: Implement log replication for same term
        %AppendEntriesResponse{term: state.current_term, success: false}
    end
  end
end 