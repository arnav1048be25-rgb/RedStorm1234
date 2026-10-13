# RedStorm v2

RedStorm is an experimental, Redis-inspired in-memory database prototype
written in Java. It is intended as a learning and engineering project
for exploring TCP servers, the RESP2 protocol, key-value storage, data
structures, and append-only-file (AOF) persistence.

> **Status:** Prototype. RedStorm is not a drop-in replacement for Redis
> and does not implement the full Redis command set or production-grade
> clustering, replication, security, or durability guarantees. Test it
> with non-critical data only.

## Features

The v2 prototype aims to provide:

-   A Java TCP server using the Redis Serialization Protocol (RESP2).
-   Redis-inspired commands for supported strings, hashes, lists, sets,
    and sorted sets.
-   Key expiration for supported commands.
-   Basic append-only-file persistence and replay for supported
    mutations.
-   Additional commands including `HSETNX`, `HINCRBYFLOAT`, `LPUSHX`,
    `RPUSHX`, `ZMSCORE`, `ZPOPMIN`, `ZPOPMAX`, and `ZRANDMEMBER`, where
    implemented in the source.

Command support and behavior may differ from Redis. Consult the source
and test each command before relying on it.

## Requirements

-   Java Development Kit (JDK) 21 or newer.
-   Optional: `redis-cli` for sending RESP-compatible commands to the
    server.

Check your Java installation:

``` bash
java -version
javac -version
```

## Compile and run

The source file name must match its public Java class name. If the
source declares `public class RedStorm`, save it as `RedStorm.java`.

Compile:

``` bash
javac RedStorm.java
```

Start the server on port 6379 with AOF persistence enabled:

``` bash
java RedStorm --port 6379 --aof redstorm.aof
```

Use the command-line options actually supported by the version of the
source you have; if these options are not recognized, inspect the
argument parsing in `RedStorm.java`.

## Connect

If `redis-cli` is installed, open another terminal and run:

``` bash
redis-cli -h 127.0.0.1 -p 6379
```

Try basic protocol/server checks:

``` text
PING
INFO
```

You can also connect with any client that can send RESP2 commands, but
unsupported commands will not behave exactly like Redis.

## Example commands

These examples are illustrative; run only commands supported by your
copy of the source:

``` text
HSETNX user:1 name Arnav
HINCRBYFLOAT user:1 balance 1.25
HGETALL user:1
ZADD leaderboard 10 alice 20 bob 15 carol
ZMSCORE leaderboard alice bob
ZPOPMIN leaderboard 2
```

## Persistence notes

AOF persistence is a prototype feature. Before using it for important
data, test restart recovery, partial/corrupt log handling, write
ordering, and crash behavior. Keep backups of any data you care about.

## Current limitations

RedStorm v2 should not be assumed to provide:

-   Full Redis command compatibility.
-   Redis Cluster, replication, or automatic failover.
-   Production-ready authentication, ACLs, or TLS.
-   Redis-equivalent concurrency, performance, or memory behavior.
-   Guaranteed crash-safe durability.

## Roadmap

Potential next milestones:

1.  Add automated command-compatibility tests.
2.  Improve concurrency and data-structure correctness.
3.  Harden AOF replay and crash recovery.
4.  Add memory limits and eviction policies.
5.  Add benchmarks against Redis using documented workloads.
6.  Implement security and operational features before production use.

## Contributing

Issues and pull requests are welcome. When reporting a bug, include:

-   Java version and operating system.
-   The command sent and the response received.
-   Steps to reproduce the issue.
-   Whether the problem persists after restarting with a fresh AOF file.

## License

No license has been specified yet. Add a `LICENSE` file before inviting
others to reuse, modify, or redistribute this project.
