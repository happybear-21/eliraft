defmodule Eliraft do
  @moduledoc """
  Eliraft is a Raft consensus implementation in Elixir.
  """

  # Types
  @type table :: atom()
  @type partition :: non_neg_integer()
  @type server :: pid() | atom()
  @type acceptor :: pid() | atom()
  @type storage :: pid() | atom()
  @type transport :: pid() | atom()
  @type queue :: pid() | atom()

  # Module attributes
  @server_name :eliraft_server
  @acceptor_name :eliraft_acceptor
  @storage_name :eliraft_storage
  @transport_name :eliraft_transport
  @queue_name :eliraft_queue
  @table :eliraft_table
  @partition 1

  # Public API

  @doc """
  Starts a new Raft server.
  """
  def start_link(opts \\ []) do
    Eliraft.Server.start_link(opts)
  end

  @doc """
  Returns the server name.
  """
  def server_name, do: @server_name

  @doc """
  Returns the acceptor name.
  """
  def acceptor_name, do: @acceptor_name

  @doc """
  Returns the storage name.
  """
  def storage_name, do: @storage_name

  @doc """
  Returns the transport name.
  """
  def transport_name, do: @transport_name

  @doc """
  Returns the queue name.
  """
  def queue_name, do: @queue_name

  @doc """
  Returns the table name.
  """
  def table, do: @table

  @doc """
  Returns the partition number.
  """
  def partition, do: @partition

  @typedoc """
  Represents an error tuple.
  """
  @type error :: {:error, term()}

  @typedoc """
  Arguments for starting a RAFT partition.
  """
  @type args :: %{
    table: table(),
    partition: partition(),
    distribution_module: module() | nil,
    log_module: module() | nil,
    label_module: module() | nil,
    storage_module: module() | nil,
    transport_module: module() | nil
  }

  @typedoc """
  Represents a Raft node identity.
  """
  @type identity :: %Eliraft.Identity{
    node: node(),
    term: non_neg_integer(),
    voted_for: node() | nil
  }

  defmodule Identity do
    @moduledoc """
    Represents the identity of a Raft node.
    """
    defstruct [:node, :term, :voted_for]
  end

  @doc """
  Hello world.

  ## Examples

      iex> Eliraft.hello()
      :world

  """
  def hello do
    :world
  end
end
