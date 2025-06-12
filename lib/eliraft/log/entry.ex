defmodule Eliraft.Log.Entry do
  @moduledoc """
  Represents a single entry in the Raft log.
  This module defines the structure and operations for log entries.
  """

  defstruct [
    :term,
    :command
  ]

  @type t :: %__MODULE__{
    term: non_neg_integer(),
    command: term()
  }

  @doc """
  Creates a new log entry.
  """
  def new(term, command) do
    %__MODULE__{
      term: term,
      command: command
    }
  end

  @doc """
  Returns the term of the entry.
  """
  def term(%__MODULE__{term: term}), do: term

  @doc """
  Returns the command of the entry.
  """
  def command(%__MODULE__{command: command}), do: command

  @doc """
  Returns true if the entry is a configuration change.
  """
  def config_change?(%__MODULE__{command: command}) do
    case command do
      {:config, _} -> true
      _ -> false
    end
  end

  @doc """
  Returns true if the entry is a no-op.
  """
  def noop?(%__MODULE__{command: nil}), do: true
  def noop?(%__MODULE__{command: :noop}), do: true
  def noop?(_), do: false
end 