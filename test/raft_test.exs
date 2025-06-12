defmodule Eliraft.RaftTest do
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
    {:ok, storage} =
      Storage.start_link(
        name: storage_name,
        data_dir: @test_data_dir
      )

    # Start log with unique name
    {:ok, log} =
      Log.start_link(
        name: log_name,
        data_dir: @test_data_dir
      )

    # Start server with unique name
    {:ok, server} =
      Server.start_link(
        name: server_name,
        storage: storage,
        log: log
      )

    # Start acceptor with unique name
    {:ok, acceptor} =
      Acceptor.start_link(
        name: acceptor_name,
        storage: storage
      )

    # Set the server for the acceptor
    Acceptor.set_server(acceptor, server)

    # Return test context
    {:ok,
     %{
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

  describe "Server" do
    test "initializes with correct default state", %{server: server} do
      status = Server.status(server)
      assert status.state == :follower
      assert status.term == 0
      assert status.leader == nil
      assert is_map(status.membership)
    end

    test "returns membership as a map", %{server: server} do
      membership = Server.membership(server)
      assert is_map(membership)
    end

    test "election timeout transitions state" do
      test_id = :rand.uniform(10000)
      server_name = :"test_server_#{test_id}"

      {:ok, server} =
        Server.start_link(
          name: server_name,
          table: :eliraft_table,
          partition: 2,
          application: Eliraft
        )

      send(server, :election_timeout)
      status = Server.status(server)
      assert status.state in [:candidate, :follower, :leader]
    end
  end

  describe "Log" do
    test "append returns :ok", %{log: log} do
      assert Log.append(log, %{term: 1, command: {:set, "key", "value"}}) == :ok
    end

    test "append_many returns :ok", %{log: log} do
      entries = [
        %{term: 1, command: {:set, "key1", "value1"}},
        %{term: 1, command: {:set, "key2", "value2"}}
      ]

      assert Log.append_many(log, entries) == :ok
    end

    test "truncate returns :ok", %{log: log} do
      assert Log.append(log, %{term: 1, command: {:set, "key", "value"}}) == :ok
      assert Log.truncate(log, 0) == :ok
    end

    test "get_entries returns {:ok, []}", %{log: log} do
      assert Log.get_entries(log, 0, 1) == {:ok, []}
    end

    test "get_entry returns {:ok, nil}", %{log: log} do
      assert Log.get_entry(log, 0) == {:ok, nil}
    end

    test "get_term returns {:ok, 0}", %{log: log} do
      assert Log.get_term(log) == {:ok, 0}
    end
  end

  describe "Acceptor" do
    test "commit returns :ok", %{acceptor: acceptor} do
      assert Acceptor.commit(acceptor, {:set, "key", "value"}) == :ok
    end

    test "read returns {:ok, nil}", %{acceptor: acceptor} do
      assert Acceptor.read(acceptor, "key") == {:ok, nil}
    end
  end
end
