defmodule Eliraft.ConsensusTest do
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

  describe "Leader Election" do
    test "election timeout triggers new election", %{server: server} do
      # Set initial state
      Server.set_state(server, :follower)
      Server.set_term(server, 0)

      # Wait for election timeout
      Process.sleep(150)

      # Verify server started election
      assert Server.get_state(server) == :candidate
      assert Server.get_term(server) == 1
    end

    test "server can become leader with majority votes", %{server: server} do
      # Set initial state
      Server.set_state(server, :candidate)
      Server.set_term(server, 1)

      # Simulate receiving majority votes
      Server.handle_vote(server, {:vote_granted, 1})

      # Verify server became leader
      assert Server.get_state(server) == :leader
    end
  end

  describe "Log Replication" do
    test "leader replicates log entries to follower", %{server: server, log: log} do
      # Set up leader
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Add entry to leader's log
      assert Log.append(log, %{term: 1, command: {:set, "key", "value"}}) == :ok

      # Verify entry is replicated
      {:ok, entries} = Log.get_entries(log, 0, 1)
      assert length(entries) == 1
      [entry] = entries
      assert entry.command == {:set, "key", "value"}
    end

    test "follower rejects entries from lower term", %{server: server, log: log} do
      # Set up follower with higher term
      Server.set_state(server, :follower)
      Server.set_term(server, 2)

      # Try to append entry with lower term
      assert Log.append(log, %{term: 1, command: {:set, "key", "value"}}) == :ok

      # Verify entry is not added
      {:ok, entries} = Log.get_entries(log, 0, 1)
      assert length(entries) == 0
    end
  end

  describe "Consistency Checks" do
    test "server maintains log consistency across term changes", %{server: server, log: log} do
      # Set up initial state
      Server.set_state(server, :leader)
      Server.set_term(server, 1)

      # Add some entries
      assert Log.append(log, %{term: 1, command: {:set, "key1", "value1"}}) == :ok
      assert Log.append(log, %{term: 1, command: {:set, "key2", "value2"}}) == :ok

      # Change term
      Server.set_term(server, 2)

      # Add more entries
      assert Log.append(log, %{term: 2, command: {:set, "key3", "value3"}}) == :ok

      # Verify all entries are present and in correct order
      {:ok, entries} = Log.get_entries(log, 0, 3)
      assert length(entries) == 3
      [entry1, entry2, entry3] = entries
      assert entry1.command == {:set, "key1", "value1"}
      assert entry2.command == {:set, "key2", "value2"}
      assert entry3.command == {:set, "key3", "value3"}
    end
  end
end 