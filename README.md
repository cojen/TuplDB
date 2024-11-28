[![Maven Central](https://img.shields.io/maven-central/v/org.cojen/tupl.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/org.cojen/tupl)

TuplDB is a high-performance, concurrent, transactional, scalable, low-level embedded database. Features include record-level locking, upgradable locks, deadlock detection, cursors, hot backups, striped files, encryption, replication, compression, nested transaction scopes, direct lock control, sorting, views, filters, transforms, unions, intersections, and full memory mapped support.

* [Javadocs](https://tupl.cojen.org/javadoc/org.cojen.tupl/org/cojen/tupl/package-summary.html)
* [FAQ](https://github.com/cojen/TuplDB/wiki/FAQ)
* [Replication](https://github.com/cojen/TuplDB/wiki/Replication)

TuplDB can be used directly, or it can be used for implementing a high-level database. TuplDB is
powerful enough for supporting all the requirements of relational SQL databases as well as
NoSQL databases. Because TuplDB doesn't impose any structure or encoding for data, a high-level
database is free to implement the most efficient format it requires.

The main entry point is the [Database](https://tupl.cojen.org/javadoc/org.cojen.tupl/org/cojen/tupl/Database.html) class. Here is a simple example for opening a non-durable database:

```java
var config = new DatabaseConfig().maxCacheSize(100_000_000);
Database db = Database.open(config);
```

To open a durable database, a base file path must be provided. Database files are created
using the base as a prefix.

```java
var config = new DatabaseConfig()
    .baseFilePath("/var/lib/tupl")
    .cacheSize(100_000_000)
    .durabilityMode(DurabilityMode.NO_FLUSH);

Database db = Database.open(config);
```

Notice that a fixed cache size is set, and also notice the durability mode. A weak
[durability mode](https://tupl.cojen.org/javadoc/org.cojen.tupl/org/cojen/tupl/DurabilityMode.html) improves
the performance of transactional changes, by not immediately flushing those
changes to the underlying files.

Setting a fixed cache size is generally preferred over setting a maximum size, because it
pre-allocates the cache when the Database is opened. This allows any heap size limits to be
detected early, yielding an OutOfMemoryError. The default cache size is 1000
pages, and the default page size is 4096 bytes.

Basic operations
----------------

A TuplDB instance manages a collection of [indexes](https://tupl.cojen.org/javadoc/org.cojen.tupl/org/cojen/tupl/Index.html), which are ordered mappings of `byte[]` keys to `byte[]` values.

```java
Database db = ...

// Open an index, creating it if necessary.
Index userIx = db.openIndex("user");
```

Indexes offer a low-level representation of data, and so applications which use them directly are
responsible for performing their own encoding.

```java
// Store a user in an auto-commit transaction.
User user = ...
byte[] userKey = encodeUserKey(user);
byte[] userValue = encodeUserValue(user);
userIx.store(null, userKey, userValue);
```

To bundle multiple operations together, specify an explicit [transaction](https://tupl.cojen.org/javadoc/org.cojen.tupl/org/cojen/tupl/Transaction.html):

```java
Index userByNameIx = ...

byte[] userNameKey = encodeUserName(user);

Transaction txn = db.newTransaction();
try {
    userIx.store(txn, userKey, userValue);
    userByNameIx.store(txn, userNameKey, userKey);
    txn.commit();
} finally {
    txn.reset();
}
```

Entries can retrieved by [loading](https://tupl.cojen.org/javadoc/org.cojen.tupl/org/cojen/tupl/View.html#load-org.cojen.tupl.Transaction-byte:A-) them directly, or via a [cursor](https://tupl.cojen.org/javadoc/org.cojen.tupl/org/cojen/tupl/Cursor.html):

```java
// Find all users whose last name starts with 'J'.
byte[] startKey = encodeUserName("J");
byte[] endKey = encodeUserName("K");

// Open a new cursor with an auto-commit transaction.
Cursor namesCursor = userByNameIx.newCursor(null);
try {
    // Find names greater than or equal to the start key.
    namesCursor.findGe(startKey);
    byte[] nameKey;
    while ((nameKey = namesCursor.key()) != null) {
        byte[] userKey = namesCursor.value();
        ...

        // Move to next name, while still being less than the end key.
        namesCursor.nextLt(endKey);
    }
} finally {
    namesCursor.reset();
}
```

The above example can also be implemented using a sub-view:

```java
// View all users whose last name starts with 'J'.
View userByNameView = userByNameIx.viewPrefix(startKey, 0);

// Scan the entire view of names.
Cursor namesCursor = userByNameView.newCursor(null);
try {
    namesCursor.first();
    byte[] nameKey;
    while ((nameKey = namesCursor.key()) != null) {
        byte[] userKey = namesCursor.value();
        ...

        namesCursor.next();
    }
} finally {
    namesCursor.reset();
}
```
