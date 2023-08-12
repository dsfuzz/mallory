# Redis-Raft Jepsen Test

This is a test suite, written using the [Jepsen distributed systems testing
library](https://jepsen.io), for
[Redis-Raft](https://github.com/RedisLabs/redisraft). It provides a single
workload (`jepsen.redis.append`) based on list append, implemented using
`LRANGE` and `RPUSH`, which uses [Elle](https://github.com/jepsen-io/elle) to find transactional anomalies up to strict serializability.

We include a wide variety of faults, including network partitions, process crashes, pauses, clock skew, and membership changes.

## Prerequisites

You'll need a Jepsen cluster running Debian 9, which you can either [build
yourself](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment)
or run in
[AWS](https://aws.amazon.com/marketplace/pp/B01LZ7Y7U0?qid=1486758124485&sr=0-1&ref_=srh_res_product_title)
via Cloudformation.

The control node needs a JVM, Graphviz, and the [Leiningen
build system](https://github.com/technomancy/leiningen#installation). The first
two you can get (on Debian) via:

```
sudo apt install openjdk8-jdk graphviz
```

Jepsen will automatically install dependencies (e.g. git, build tools, various
support libraries) on DB nodes, and clone and build both Redis and Redis-Raft
locally on each DB node (to avoid cross-compilation issues). To do this, root
on each DB node will need an SSH key with access to the Redis-Raft repo on
GitHub, and the GitHub public SSH key in `~/.ssh/known_hosts`---see below.

## Usage

To get started, try

```
lein run test-all
```

To test a particular GIT SHA of Redis or Redis-Raft, pass `--version` or
`--raft-version`, respectively. You may also want to specify a particular
username and what DB nodes you'd like to use, and provide a custom
`--time-limit` for each test. To run several iterations of each test, use
`--test-count`. A thorough test run might look like:

```
lein run test-all --username admin --nodes-file ~/nodes --raft-version 73ad833 --time-limit 600 --test-count 10
```

To focus on a particular set of faults, use `--nemesis`

```
lein run test-all --nemesis partition,kill
```

To see all options, try

```
lein run test-all --help
```

## FAQ

### Host key verification failed

```
Cloning into 'redis'...
Host key verification failed.
fatal: Could not read from remote repository.
```

Redis-Raft is a private repo, and we clone it directly from Github. That means
your root user on each DB node will need an SSH key with read access to that
repo installed on each DB node, and git on each DB node will need to know about
the SSH key for GitHub. You should verify the host key from GH directly, rather
than keyscanning it. If you like to live dangerously, which, again, you should
not, you can blindly accept whatever key you get via:

```
ssh n1 "sudo bash -c 'ssh-keyscan github.com >> ~/.ssh/known_hosts'"
ssh n2 ...
ssh n3 ...
...
```

## License

Copyright © 2020 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
