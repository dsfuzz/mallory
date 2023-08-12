# Dqlite Jepsen Test

A Clojure library designed to test Dqlite, an embedded SQL database with Raft
consensus.

## What is being tested?

The tests run concurrent operations to some shared data from different nodes in
a Dqlite cluster, checking that the operations preserve the consistency
properties defined in each test.  During the tests, various combinations of
nemeses can be added to interfere with the database operations and exercise the
database's consistency protocols.

## Running

To run a single test, try

```
lein run test --workload sets --nemesis kill --time-limit 60 --test-count 1 --concurrency 2n
```

To run the full suite, use

```
lein run test-all
```

See `lein run test --help` and `lein run test-all --help` for options.

#### Workloads

+ **append** Checks for dependency cycles in append/read transactions
+ **set** concurrent unique appends to a single table

#### Nemeses

+ **none** no nemesis
+ **kill** kills random Dqlite test application processes
+ **partition** network partitions
+ **pause** process pauses
+ **member** remove or re-add a node

#### Time Limit

Time to run test, usually 60, 180, ... seconds

#### Test Count

Times to run test, should >= 1

#### Concurrency

Number of threads. 2n means "twice the number of nodes", and is a good default.

