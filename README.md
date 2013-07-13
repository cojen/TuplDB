Tupl
====

The Unnamed Persistence Library

Tupl is a high-performance, concurrent, transactional, scalable, low-level database. Intended to replace BerkeleyDB, Tupl supports true record-level locking. Unlike BerkeleyDB-JE, Tupl doesn't suffer from garbage collection pauses with very large databases. Tupl also includes support for cursors, upgradable locks, deadlock detection, hot backups, striped files, encryption, nested transaction scopes, and direct lock control.

* [Javadocs](http://cojen.github.com/Tupl/javadoc/org/cojen/tupl/package-summary.html)

The main entry point is the [Database](http://cojen.github.io/Tupl/javadoc/org/cojen/tupl/Database.html) class. Here is a simple example for opening a non-durable database:

```java
DatabaseConfig config = new DatabaseConfig().maxCacheSize(100_000_000);
Database db = Database.open(config);
```

To open a durable database, a base file path must be provided. Database files are created
using the base as a prefix.

```java
DatabaseConfig config = new DatabaseConfig()
    .baseFilePath("/var/lib/tupl")
    .minCacheSize(100_000_000)
    .durabilityMode(DurabilityMode.NO_FLUSH);

Database db = Database.open(config);
```

Notice that a minimum cache size is set, and also notice the durability mode. A weak
[durability mode](http://cojen.github.io/Tupl/javadoc/org/cojen/tupl/DurabilityMode.html) improves
the performance of transactional changes, by not immediately flushing those
changes to the underlying files.

Setting a minimum cache size is generally preferred over setting a maximum size, because it
pre-allocates the cache when the Database is opened. This allows any heap size limits to be
detected early, yielding an OutOfMemoryError. When not specified, the maximum cache size
matches the minimum cache size. When neither is specified, the default cache size is 1000
pages, where a page is 4096 bytes by default.


