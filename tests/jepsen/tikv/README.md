# jepsen.tikv

Jepsen test for TiKV.

## Usage

1. Build rust-client-server
```bash
make build-client-rust-server
```

2. Run the test on nodes:
```bash
lein run test --workload register --concurrency 10 --time-limit 30
```
3. Workload
- **register** concurrent atomic updates to multiple shared registers
- **set** concurrent unique appends to a single key
- **list-append** Checks for dependency cycles in append/read transactions

## Architecture

When we run a Jepsen test, we need several test nodes to deploy our databases, and a control node which runs the tests written in Clojure. Under our circustance, we setup 5 CentOS 7 nodes via LXC and make sure it can be logged in via ssh from control node, which is the host.

The whole process of running a Jepsen test consist of four phases. First, we try to tear down the database if it's already running, and setup a brand-new database on test nodes. And then, we run specific workloads on it and record everything we observe from the client-side as a history. Third, we analyze the history with a specific model and a checker to see whether it shows any anomolies. Finally, we tear down the databases (This pharse will be skipped if `--leave-db-running` option is present).

Each workload usually has more than one concurrent unit. In Jepsen, it is called a process. Each process executes operations in a blocking way. Each process create a client connected to a specific node for operations.

Normally, the client is implemented natively in Clojure. But in our case, we implement it in a "client server" way.
While setting up each test node, we also start a "client server" connected to the node. The "client server" exposes a RPC service for the TiKV client implemented in Clojure.

```
┌───────────────────────────────────────────┐   ┌─────────────┐
│                                           │   │             │
│                               ┌────────┐  │   │   ┌──────┐  │
│    ┌─────────────────┐   ┌───►│ client ├──┼───┼──►│ node │  │
│    │     Jepsen      │   │    │ server │  │   │   └──────┘  │
│    │                 │   │    └────────┘  │   │             │
│    │   ┌─────────┐   │   │                │   │             │
│    │   │ Process ├───┼───┘    ┌────────┐  │   │   ┌──────┐  │
│    │   └─────────┘   │        │ client ├──┼───┼──►│ node │  │
│    │                 │        │ server │  │   │   └──────┘  │
│    │   ┌─────────┐   │        └────────┘  │   │             │
│    │   │ Process ├───┼───┐                │   │             │
│    │   └─────────┘   │   │    ┌────────┐  │   │   ┌──────┐  │
│    │       .         │   └───►│ client ├──┼───┼──►│ node │  │
│    │       .         │        │ server │  │   │   └──────┘  │
│    │       .         │        └────────┘  │   │             │
│    │       .         │                    │   │             │
│    │       .         │        ┌────────┐  │   │   ┌──────┐  │
│    │       .         │        │ client ├──┼───┼──►│ node │  │
│    │   ┌─────────┐   │        │ server │  │   │   └──────┘  │
│    │   │ Process ├───┼───┐    └────────┘  │   │             │
│    │   └─────────┘   │   │                │   │             │
│    │                 │   │    ┌────────┐  │   │   ┌──────┐  │
│    │                 │   └───►│ client ├──┼───┼──►│ node │  │
│    └─────────────────┘        │ server │  │   │   └──────┘  │
│                               └────────┘  │   │             │
│                                           │   │             │
│                    Host                   │   │     LXC     │
└───────────────────────────────────────────┘   └─────────────┘
```

## License

TiKV is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
