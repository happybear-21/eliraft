defmodule Eliraft.Config do
  @moduledoc """
  Manages the configuration for a Raft cluster.
  This includes membership information and cluster settings.
  """

  defstruct [
    :version,
    :members,
    :witnesses,
    :full_members
  ]

  @type t :: %__MODULE__{
    version: non_neg_integer(),
    members: %{node() => boolean()},
    witnesses: [node()],
    full_members: [node()]
  }

  @doc """
  Creates a new empty configuration.
  """
  def new do
    %__MODULE__{
      version: 0,
      members: %{},
      witnesses: [],
      full_members: []
    }
  end

  @doc """
  Creates a new configuration with the given members.
  """
  def new(members) when is_list(members) do
    member_map = Enum.into(members, %{}, &{&1, true})
    %__MODULE__{
      version: 0,
      members: member_map,
      witnesses: [],
      full_members: members
    }
  end

  @doc """
  Creates a new configuration with the given members and witnesses.
  """
  def new(members, witnesses) when is_list(members) and is_list(witnesses) do
    member_map = Enum.into(members ++ witnesses, %{}, &{&1, true})
    %__MODULE__{
      version: 0,
      members: member_map,
      witnesses: witnesses,
      full_members: members
    }
  end

  @doc """
  Returns true if the given node is a data replica.
  """
  def is_data_replica?(%__MODULE__{full_members: full_members}, node) do
    node in full_members
  end

  @doc """
  Returns true if the given node is a witness.
  """
  def is_witness?(%__MODULE__{witnesses: witnesses}, node) do
    node in witnesses
  end

  @doc """
  Returns the current configuration version.
  """
  def version(%__MODULE__{version: version}), do: version

  @doc """
  Returns the list of all members in the cluster.
  """
  def members(%__MODULE__{members: members}), do: Map.keys(members)

  @doc """
  Returns the list of witness nodes.
  """
  def witnesses(%__MODULE__{witnesses: witnesses}), do: witnesses

  @doc """
  Returns the list of full members (non-witness nodes).
  """
  def full_members(%__MODULE__{full_members: full_members}), do: full_members
end 