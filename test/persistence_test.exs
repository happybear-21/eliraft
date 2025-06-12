defmodule Eliraft.PersistenceTest do
  use ExUnit.Case

  alias Eliraft.{Storage, Storage.Disk, Log, Server}

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

    # Ensure no process is registered with these names
    for name <- [storage_name, log_name, server_name] do
      if pid = Process.whereis(name), do: Process.exit(pid, :kill)
    end

    log_data_dir = Path.join(@test_data_dir, "log")
    storage_data_dir = Path.join(@test_data_dir, "storage")
    File.mkdir_p!(log_data_dir)
    File.mkdir_p!(storage_data_dir)

    # Start storage with unique name
    {:ok, storage} = Storage.start_link(
      name: storage_name,
      data_dir: storage_data_dir
    )

    # Start log with unique name
    {:ok, log} = Log.start_link(
      name: log_name,
      data_dir: log_data_dir
    )

    # Start server with unique name
    {:ok, server} = Server.start_link(
      name: server_name,
      storage: storage,
      log: log
    )

    # Return test context
    {:ok, %{
      storage: storage,
      log: log,
      server: server,
      storage_name: storage_name,
      log_name: log_name,
      server_name: server_name,
      log_data_dir: log_data_dir,
      storage_data_dir: storage_data_dir
    }}
  end

  describe "Disk Storage" do
    test "persists and recovers log entries", %{log: log, log_data_dir: log_data_dir} do
      # Add some entries
      assert Log.append(log, %{term: 1, command: {:set, "key1", "value1"}}) == :ok
      assert Log.append(log, %{term: 1, command: {:set, "key2", "value2"}}) == :ok

      # Get entries
      {:ok, entries} = Log.get_entries(log, 0, 2)
      assert length(entries) == 2
      [entry1, entry2] = entries
      assert entry1.command == {:set, "key1", "value1"}
      assert entry2.command == {:set, "key2", "value2"}

      # Restart log server
      Process.exit(log, :normal)
      {:ok, new_log} = Log.start_link(
        name: :"test_log_#{:rand.uniform(10000)}",
        data_dir: log_data_dir
      )

      # Verify entries are recovered
      {:ok, recovered_entries} = Log.get_entries(new_log, 0, 2)
      assert length(recovered_entries) == 2
      [recovered1, recovered2] = recovered_entries
      assert recovered1.command == {:set, "key1", "value1"}
      assert recovered2.command == {:set, "key2", "value2"}
    end

    test "persists and recovers state machine", %{storage: storage, storage_data_dir: storage_data_dir} do
      # Apply some commands
      assert Storage.apply(storage, {:set, "key1", "value1"}, 1, :test) == :ok
      assert Storage.apply(storage, {:set, "key2", "value2"}, 2, :test) == :ok

      # Read values
      assert Storage.read(storage, "key1") == {:ok, "value1"}
      assert Storage.read(storage, "key2") == {:ok, "value2"}

      # Restart storage server
      Process.exit(storage, :normal)
      {:ok, new_storage} = Storage.start_link(
        name: :"test_storage_#{:rand.uniform(10000)}",
        data_dir: storage_data_dir
      )

      # Verify state is recovered
      assert Storage.read(new_storage, "key1") == {:ok, "value1"}
      assert Storage.read(new_storage, "key2") == {:ok, "value2"}
    end

    test "handles log truncation with persistence", %{log: log, log_data_dir: log_data_dir} do
      # Add entries
      assert Log.append(log, %{term: 1, command: {:set, "key1", "value1"}}) == :ok
      assert Log.append(log, %{term: 1, command: {:set, "key2", "value2"}}) == :ok
      assert Log.append(log, %{term: 1, command: {:set, "key3", "value3"}}) == :ok

      # Truncate log
      assert Log.truncate(log, 1) == :ok

      # Verify truncation
      {:ok, entries} = Log.get_entries(log, 0, 1)
      assert length(entries) == 1
      [entry] = entries
      assert entry.command == {:set, "key1", "value1"}

      # Restart log server
      Process.exit(log, :normal)
      {:ok, new_log} = Log.start_link(
        name: :"test_log_#{:rand.uniform(10000)}",
        data_dir: log_data_dir
      )

      # Verify truncation persisted
      {:ok, recovered_entries} = Log.get_entries(new_log, 0, 1)
      assert length(recovered_entries) == 1
      [recovered] = recovered_entries
      assert recovered.command == {:set, "key1", "value1"}
    end

    test "handles concurrent writes with persistence", %{log: log, storage: storage, log_data_dir: log_data_dir} do
      # Set up server as leader
      Server.set_state(storage, :leader)
      Server.set_term(storage, 1)

      # Perform concurrent writes
      tasks = [
        Task.async(fn -> 
          Log.append(log, %{term: 1, command: {:set, "key1", "value1"}})
        end),
        Task.async(fn -> 
          Log.append(log, %{term: 1, command: {:set, "key2", "value2"}})
        end),
        Task.async(fn -> 
          Log.append(log, %{term: 1, command: {:set, "key3", "value3"}})
        end)
      ]

      # Wait for all tasks to complete
      Enum.each(tasks, &Task.await/1)

      # Verify all entries are persisted
      {:ok, entries} = Log.get_entries(log, 0, 3)
      assert length(entries) == 3

      # Restart servers
      Process.exit(log, :normal)
      Process.exit(storage, :normal)

      {:ok, new_log} = Log.start_link(
        name: :"test_log_#{:rand.uniform(10000)}",
        data_dir: log_data_dir
      )

      # Verify all entries are recovered
      {:ok, recovered_entries} = Log.get_entries(new_log, 0, 3)
      assert length(recovered_entries) == 3
    end

    test "handles crash recovery with partial writes", %{log: log, log_data_dir: log_data_dir} do
      # Add some entries
      assert Log.append(log, %{term: 1, command: {:set, "key1", "value1"}}) == :ok
      assert Log.append(log, %{term: 1, command: {:set, "key2", "value2"}}) == :ok

      # Simulate crash during write
      Process.exit(log, :kill)

      # Restart log server
      {:ok, new_log} = Log.start_link(
        name: :"test_log_#{:rand.uniform(10000)}",
        data_dir: log_data_dir
      )

      # Verify entries are recovered correctly
      {:ok, entries} = Log.get_entries(new_log, 0, 2)
      assert length(entries) == 2
      [entry1, entry2] = entries
      assert entry1.command == {:set, "key1", "value1"}
      assert entry2.command == {:set, "key2", "value2"}
    end
  end
end 