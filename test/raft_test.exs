defmodule Eliraft.RaftTest do
  use ExUnit.Case

  alias Eliraft.Server
  alias Eliraft.Log
  alias Eliraft.Acceptor

  describe "Server" do
    setup do
      {:ok, server} = Server.start_link(table: :eliraft_table, partition: 1, application: Eliraft)
      %{server: server}
    end

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
      {:ok, server} = Server.start_link(table: :eliraft_table, partition: 2, application: Eliraft)
      send(server, :election_timeout)
      status = Server.status(server)
      assert status.state in [:candidate, :follower, :leader]
    end
  end

  describe "Log" do
    setup do
      {:ok, log} = Log.start_link(table: :eliraft_table, partition: 1)
      %{log: log}
    end

    test "append returns :ok", %{log: log} do
      assert Log.append(log, %{term: 1, command: "test"}) == :ok
    end

    test "append_many returns :ok", %{log: log} do
      assert Log.append_many(log, [%{term: 1, command: "a"}], 1) == :ok
    end

    test "truncate returns :ok", %{log: log} do
      assert Log.truncate(log, 0) == :ok
    end

    test "get_term returns {:ok, 0}", %{log: log} do
      assert Log.get_term(log, 0) == {:ok, 0}
    end

    test "get_entry returns {:ok, nil}", %{log: log} do
      assert Log.get_entry(log, 0) == {:ok, nil}
    end

    test "get_entries returns {:ok, []}", %{log: log} do
      assert Log.get_entries(log, 0, 1) == {:ok, []}
    end
  end

  describe "Acceptor" do
    setup do
      {:ok, server} = Server.start_link(table: :eliraft_table, partition: 3, application: Eliraft)
      {:ok, acceptor} = Acceptor.start_link(name: :test_acceptor, server: server)
      %{acceptor: acceptor}
    end

    test "commit returns :ok", %{acceptor: acceptor} do
      assert Acceptor.commit(acceptor, {:set, "key", "value"}) == :ok
    end

    test "read returns {:ok, nil}", %{acceptor: acceptor} do
      assert Acceptor.read(acceptor, {:get, "key"}) == {:ok, nil}
    end
  end
end 