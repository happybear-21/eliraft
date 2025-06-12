defmodule Eliraft.CommitTest do
  use ExUnit.Case

  alias Eliraft.Server
  alias Eliraft.Log
  alias Eliraft.Acceptor

  describe "Commit Operations" do
    setup do
      {:ok, log} = Log.start_link([])
      {:ok, server} = Server.start_link(log: log, name: :test_server)
      {:ok, acceptor} = Acceptor.start_link(name: :test_acceptor, server: server)
      %{server: server, acceptor: acceptor, log: log}
    end

    test "commit operation is persisted to log", %{acceptor: acceptor, server: server} do
      # First, ensure we're in a state where we can commit
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Perform a commit operation
      assert Acceptor.commit(acceptor, {:set, "test_key", "test_value"}) == :ok

      # Verify the operation was logged
      {:ok, entries} = Log.get_entries(server, 0, 1)
      assert length(entries) > 0
      [entry | _] = entries
      assert entry.command == {:set, "test_key", "test_value"}
      assert entry.term == 1
    end

    test "commit operation is rejected when not leader", %{acceptor: acceptor, server: server} do
      # Ensure we're not in leader state
      Server.set_state(server, :follower)
      
      # Attempt to commit should fail
      assert Acceptor.commit(acceptor, {:set, "test_key", "test_value"}) == {:error, :not_leader}
    end

    test "commit operation is replicated to followers", %{acceptor: acceptor, server: server} do
      # Set up leader state
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Perform multiple commits
      assert Acceptor.commit(acceptor, {:set, "key1", "value1"}) == :ok
      assert Acceptor.commit(acceptor, {:set, "key2", "value2"}) == :ok

      # Verify all entries are in log
      {:ok, entries} = Log.get_entries(server, 0, 2)
      assert length(entries) == 2
      [entry1, entry2] = entries
      assert entry1.command == {:set, "key1", "value1"}
      assert entry2.command == {:set, "key2", "value2"}
    end

    test "commit index advances correctly", %{acceptor: acceptor, server: server} do
      # Set up leader state
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Perform commits
      assert Acceptor.commit(acceptor, {:set, "key1", "value1"}) == :ok
      assert Acceptor.commit(acceptor, {:set, "key2", "value2"}) == :ok

      # Verify commit index
      status = Server.status(server)
      assert status.commit_index >= 2
    end

    test "commit operation with invalid command is rejected", %{acceptor: acceptor, server: server} do
      # Set up leader state
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Attempt to commit invalid command
      assert Acceptor.commit(acceptor, :invalid_command) == {:error, :invalid_command}
    end
  end
end 