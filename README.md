# Eliraft

Eliraft is an implementation of the Raft consensus algorithm in Elixir.

## Prerequisites

- **Elixir** (version 1.18 or later)
- **Erlang/OTP** (compatible with your Elixir version)

## Getting Started

### 1. Clone the Repository

```sh
git clone https://github.com/happybear-21/eliraft.git
cd eliraft
```

### 2. Install Dependencies

Fetch and install all dependencies:

```sh
mix deps.get
```

### 3. Compile the Project

Compile the source code:

```sh
mix compile
```

### 4. Run the Server

You can run the application in two ways:

#### a) Interactive Mode (Recommended for Development)

Start an interactive Elixir shell with your application running:

```sh
iex -S mix
```

#### b) Non-Interactive Mode

Run the application without an interactive shell:

```sh
mix run --no-halt
```

### 5. Interacting with the Raft Server

Once the application is running (especially in IEx), you can interact with the Raft server:

```elixir
# Check the status of the server
Eliraft.Server.status(:eliraft_table_1)

# Check the membership
Eliraft.Server.membership(:eliraft_table_1)
```

You can also start an acceptor and perform commit/read operations:

```elixir
{:ok, acceptor} = Eliraft.Acceptor.start_link(name: :test_acceptor, server: :eliraft_table_1)
Eliraft.Acceptor.commit(acceptor, {:set, "key", "value"})
Eliraft.Acceptor.read(acceptor, {:get, "key"})
```

## Features

| Feature | Status | Notes |
|---------|--------|-------|
| Leader Election + Log Replication | ✅ Implemented | Basic leader election and log replication are implemented with tests. Includes term management, vote handling, and log consistency checks. |
| Persistence | 🟡 Partial | Basic log persistence is implemented, but lacks disk-based persistence and crash recovery. |
| Membership Changes | 🟡 Partial | Basic membership management is implemented in Config module, but lacks dynamic cluster reconfiguration. |
| Log Compaction | ❌ Not Implemented | No implementation of log compaction or snapshot mechanism. |

## Development

- To run tests:  
  ```sh
  mix test
  ```
- To generate documentation:  
  ```sh
  mix docs
  ```

## License
 GPL-3.0 license