defmodule Eliraft.Log.View do
  @moduledoc """
  Represents a view of the Raft log.
  This module manages the logical view of the log, including its start and end indices.
  """

  defstruct [
    :log,
    :first,
    :last,
    :config
  ]

  @type t :: %__MODULE__{
    log: term(),
    first: non_neg_integer(),
    last: non_neg_integer(),
    config: nil | {non_neg_integer(), term()}
  }

  @doc """
  Creates a new empty log view.
  """
  def new do
    %__MODULE__{
      log: nil,
      first: 0,
      last: 0,
      config: nil
    }
  end

  @doc """
  Creates a new log view with the given parameters.
  """
  def new(log, first, last, config \\ nil) do
    %__MODULE__{
      log: log,
      first: first,
      last: last,
      config: config
    }
  end

  @doc """
  Updates the first index of the view.
  """
  def update_first(view, first) do
    %{view | first: first}
  end

  @doc """
  Updates the last index of the view.
  """
  def update_last(view, last) do
    %{view | last: last}
  end

  @doc """
  Updates the configuration of the view.
  """
  def update_config(view, config) do
    %{view | config: config}
  end

  @doc """
  Returns true if the view is empty.
  """
  def empty?(%__MODULE__{first: first, last: last}) do
    first > last
  end

  @doc """
  Returns the number of entries in the view.
  """
  def size(%__MODULE__{first: first, last: last}) do
    last - first + 1
  end
end 