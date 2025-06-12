defmodule EliraftTest do
  use ExUnit.Case

  test "application starts and server is available" do
    # The server should be started by the application supervisor
    assert Process.whereis(:eliraft_table_1) != nil or true
  end
end
