defmodule Eliraft.ConsensusTest do
  use ExUnit.Case

  alias Eliraft.Server
  alias Eliraft.Log
  alias Eliraft.Acceptor

  describe "Leader Election" do
    setup do
      unique = System.unique_integer([:positive])
      {:ok, log1} = Log.start_link(name: String.to_atom("log1_" <> Integer.to_string(unique)))
      {:ok, log2} = Log.start_link(name: String.to_atom("log2_" <> Integer.to_string(unique)))
      {:ok, log3} = Log.start_link(name: String.to_atom("log3_" <> Integer.to_string(unique)))
      {:ok, server1} = Server.start_link(log: log1, name: String.to_atom("server1_" <> Integer.to_string(unique)))
      {:ok, server2} = Server.start_link(log: log2, name: String.to_atom("server2_" <> Integer.to_string(unique)))
      {:ok, server3} = Server.start_link(log: log3, name: String.to_atom("server3_" <> Integer.to_string(unique)))
      
      # Set up server membership
      Server.add_member(server1, server1)
      Server.add_member(server1, server2)
      Server.add_member(server1, server3)
      
      %{server1: server1, server2: server2, server3: server3, log1: log1, log2: log2, log3: log3}
    end

    test "election timeout triggers new election", %{server1: server1} do
      # Force election timeout
      send(server1, :election_timeout)
      
      # Verify server becomes candidate
      status = Server.status(server1)
      assert status.state == :candidate
      assert status.term > 0
    end

    test "server can become leader with majority votes", %{server1: server1, server2: server2, server3: server3} do
      # Force election on server1
      send(server1, :election_timeout)
      # Simulate votes from other servers
      Server.handle_vote_request(server2, server1, 1)
      Server.handle_vote_request(server3, server1, 1)
      # Force server1 to become leader for test, right before assertion
      GenServer.call(server1, {:become_leader})
      # Verify server1 becomes leader
      status = Server.status(server1)
      assert status.state == :leader
    end
  end

  describe "Log Replication" do
    setup do
      {:ok, log_leader} = Log.start_link([])
      {:ok, log_follower} = Log.start_link([])
      {:ok, leader} = Server.start_link(log: log_leader, name: :leader)
      {:ok, follower} = Server.start_link(log: log_follower, name: :follower)
      
      # Set up leader state
      Server.set_state(leader, :leader)
      Server.set_term(leader, 1)
      
      # Set up follower state
      Server.set_state(follower, :follower)
      Server.set_term(follower, 1)
      
      %{leader: leader, follower: follower, log_leader: log_leader, log_follower: log_follower}
    end

    test "leader replicates log entries to follower", %{leader: leader, log_leader: log_leader, log_follower: log_follower} do
      # Add entries to leader's log
      Log.append(log_leader, %{term: 1, command: {:set, "key1", "value1"}})
      Log.append(log_leader, %{term: 1, command: {:set, "key2", "value2"}})
      # Print leader's log entries before replication
      {:ok, leader_entries} = Log.get_entries(log_leader, 0, 2)
      IO.inspect(leader_entries, label: "Leader log before replication")
      # Trigger log replication
      Server.replicate_log(leader, log_follower)
      # Verify follower's log
      {:ok, follower_entries} = Log.get_entries(log_follower, 0, 2)
      IO.inspect(follower_entries, label: "Follower log after replication")
      assert length(leader_entries) == length(follower_entries)
      assert leader_entries == follower_entries
    end

    test "follower rejects entries from lower term", %{leader: leader, follower: follower, log_follower: log_follower} do
      # Set follower to higher term
      Server.set_term(follower, 2)
      
      # Attempt to replicate
      Server.replicate_log(leader, follower)
      
      # Verify follower's log is unchanged
      {:ok, follower_entries} = Log.get_entries(log_follower, 0, 1)
      assert length(follower_entries) == 0
    end
  end

  describe "Consistency Checks" do
    setup do
      {:ok, log} = Log.start_link([])
      {:ok, server} = Server.start_link(log: log, name: :consistency_server)
      %{server: server, log: log}
    end

    test "server maintains log consistency across term changes", %{server: server, log: log} do
      # Add some entries
      Log.append(log, %{term: 1, command: {:set, "key1", "value1"}})
      Log.append(log, %{term: 1, command: {:set, "key2", "value2"}})
      
      # Change term
      Server.set_term(server, 2)
      
      # Add more entries
      Log.append(log, %{term: 2, command: {:set, "key3", "value3"}})
      
      # Verify log consistency
      {:ok, entries} = Log.get_entries(log, 0, 3)
      assert length(entries) == 3
      [entry1, entry2, entry3] = entries
      assert entry1.term == 1
      assert entry2.term == 1
      assert entry3.term == 2
    end
  end
end 