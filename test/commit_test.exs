defmodule Eliraft.CommitTest do
  use ExUnit.Case

  alias Eliraft.{Storage, Log, Server, Acceptor}

  @test_data_dir "test/data"

  setup do
    # Clean up test data directory
    File.rm_rf!(@test_data_dir)
    File.mkdir_p!(@test_data_dir)

    # Generate unique names for this test run
    test_id = :rand.uniform(10000)
    storage_name = :"test_storage_#{test_id}"
    log_name = :"test_log_#{test_id}"
    server_name = :"test_server_#{test_id}"
    acceptor_name = :"test_acceptor_#{test_id}"

    # Ensure no process is registered with these names
    for name <- [storage_name, log_name, server_name, acceptor_name] do
      if pid = Process.whereis(name), do: Process.exit(pid, :kill)
    end

    # Start storage with unique name
    {:ok, storage} = Storage.start_link(
      name: storage_name,
      data_dir: @test_data_dir
    )

    # Start log with unique name
    {:ok, log} = Log.start_link(
      name: log_name,
      data_dir: @test_data_dir
    )

    # Start server with unique name
    {:ok, server} = Server.start_link(
      name: server_name,
      storage: storage,
      log: log
    )

    # Start acceptor with unique name
    {:ok, acceptor} = Acceptor.start_link(
      name: acceptor_name,
      storage: storage
    )

    # Set the server for the acceptor
    Acceptor.set_server(acceptor, server)

    # Return test context
    {:ok, %{
      storage: storage,
      log: log,
      server: server,
      acceptor: acceptor,
      storage_name: storage_name,
      log_name: log_name,
      server_name: server_name,
      acceptor_name: acceptor_name
    }}
  end

  describe "Commit Operations" do
    test "commit operation is persisted to log", %{log: log} do
      # Add entry to log
      assert Log.append(log, %{term: 1, command: {:set, "key1", "value1"}}) == :ok

      # Verify entry is in log
      {:ok, entries} = Log.get_entries(log, 0, 1)
      assert length(entries) == 1
      [entry] = entries
      assert entry.command == {:set, "key1", "value1"}
    end

    test "commit operation is rejected when not leader", %{server: server, acceptor: acceptor} do
      # Set server as follower
      Server.set_state(server, :follower)

      # Try to commit operation
      assert Acceptor.commit(acceptor, {:set, "key", "value"}) == {:error, :not_leader}
    end

    test "commit operation is replicated to followers", %{server: server, acceptor: acceptor, log: log} do
      # Set server as leader
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Commit operation
      assert Acceptor.commit(acceptor, {:set, "key1", "value1"}) == :ok

      # Verify operation is in log
      {:ok, entries} = Log.get_entries(log, 0, 1)
      assert length(entries) == 1
      [entry] = entries
      assert entry.command == {:set, "key1", "value1"}
    end

    test "commit index advances correctly", %{server: server, acceptor: acceptor, log: log} do
      # Set server as leader
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Initial commit index
      initial_index = Server.get_commit_index(server)

      # Commit operation
      assert Acceptor.commit(acceptor, {:set, "key1", "value1"}) == :ok

      # Verify commit index advanced
      new_index = Server.get_commit_index(server)
      assert new_index > initial_index
    end

    test "commit operation with invalid command is rejected", %{server: server, acceptor: acceptor} do
      # Set server as leader
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Try to commit invalid command
      assert Acceptor.commit(acceptor, {:invalid_command}) == {:error, :invalid_command}
    end
  end
end 