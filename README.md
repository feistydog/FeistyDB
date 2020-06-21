# FeistyDB

[![Build Status](https://travis-ci.org/feistydog/FeistyDB.svg?branch=master)](https://travis-ci.org/feistydog/FeistyDB)

A powerful and performant Swift interface to [SQLite](https://sqlite.org) featuring:

- Type-safe and type-agnostic database values.
- Thread-safe synchronous and asynchronous database access.
-  Full support for [transactions](#perform-a-transaction) and savepoints.
- [Custom SQL functions](#custom-sql-functions), including aggregate and window functions.
- [Custom collating sequences](#custom-collating-sequences).
- Custom commit, rollback, update, and busy handler hooks.
- Custom virtual tables.
- Custom FTS5 tokenizers.

FeistyDB allows fast, easy database access with robust error handling.  It is not a general-purpose object-relational mapper.

## Installation

1. Clone the [FeistyDB](https://github.com/feistydog/FeistyDB) repository.
2. Run `./get-sqlite.sh` to download the latest SQLite source tree and build the [amalgamation](https://sqlite.org/amalgamation.html) with the `uuid` and `carray` extensions added.
3. Open the project, build, and get started in the playground!

## Quick Start

```swift
// Create an in-memory database
let db = try Database()

// Create a table
try db.execute(sql: "CREATE TABLE t1(a,b);")

// Insert a row
try db.execute(sql: "INSERT INTO t1(a,b) VALUES (?, ?);", 
   parameterValues: [33, "lulu"])

// Retrieve the values
try db.execute(sql: "SELECT a,b FROM t1;") { row in
    let a: Int = try row.value(at: 0)
    let b: String = try row.value(at: 1)
}
```

### Segue to Thread-Safety

`FeistyDB` compiles SQLite with thread safety disabled for improved performance. While this increases performance, it also means a `Database` instance may only be accessed from a single thread or dispatch queue at a time.

Most applications should not create a `Database` directly but instead should use a thread-safe `DatabaseQueue`.

```swift
// Create a queue serializing access to an in-memory database
let dbQ = try DatabaseQueue("myapp.dbQ")
```

This creates a queue which may be used from multiple threads safely.  The queue serializes access to the database ensuring only a single operation occurs at a time. Database operations may be performed synchronously:

```swift
try dbQ.sync { db in
    // Do something with `db`
}
```

or asynchronously:

```swift
dbQ.async { db in
    do {
        // Do something with `db`
    } 
    catch let error {
        // Handle any errors that occurred
    }
}
```

For databases using [Write-Ahead Logging](https://www.sqlite.org/wal.html) concurrent reading and writing is supported. Multiple read operations may be performed simultaneously using more than one `DatabaseReadQueue` instance.  Write operations must always be confined to a single `DatabaseQueue`.  A typical usage pattern is one global `DatabaseQueue` instance used for writing located in the application's delegate, with `DatabaseReadQueue` instances located in individual view or window controllers.  When used with long-running read transactions each `DatabaseReadQueue` maintains a separate, consistent snapshot of the database that may be updated in response to database changes.

## Design

The core of FeistyDB is the types `Database`, `Statement`, and `Row`.

- `Database` is an SQLite database.

- `Statement` is a compiled SQL statement.

- `Row` is a single result row.

The fundamental type for native database values is `DatabaseValue`.

- `DatabaseValue` contains an integer, floating-point, textual, or blob value.

Type-safe access to database values is provided by classes implementing the `ColumnConvertible` protocol.

- `ColumnConvertible` is a type that can be initialized from a column in a result row.

SQL parameter binding is provided by classes implementing the `ParameterBindable` protocol.

- `ParameterBindable ` is a type that can bind its value to an SQL parameter.

General object storage is provided by classes implementing the `DatabaseSerializable` protocol.

- `DatabaseSerializable ` is a type that can be serialized to and deserialized from a database column.

Thread-safe access to a database is provided by `DatabaseQueue`.

- `DatabaseQueue` serializes work items on a database.
- `DatabaseReadQueue` serializes read operations on a database.

## Examples

### Create an In-Memory Database

```swift
let db = try Database()
```

This creates a database for use on a single thread or queue only. Most applications should not create a `Database` directly but instead should use a thread-safe `DatabaseQueue`.

### Create a Table

```swift
try db.execute(sql: "CREATE TABLE t1(a,b);")
```

The created table `t1` has two columns, `a` and `b`.

### Insert Data

```swift
for i in 0..<5 {
    try db.execute(sql: "INSERT INTO t1(a,b) VALUES (?,?);",
	   parameterValues: [2*i, 2*i+1])
}
```
SQL parameters are passed as a sequence or series of values.  Named parameters are also supported.

```swift
try db.execute(sql: "INSERT INTO t1(a,b) VALUES (:a,:b);",
		parameters: [":a": 100, ":b": 404])
```

### Insert Data Efficiently

Rather than parsing SQL each time a statement is executed, it is more efficient to prepare a statement and reuse it.

```swift
let s = try db.prepare(sql: "INSERT INTO t1(a,b) VALUES (?,?);")
for i in 0..<5 {
    try s.bind(parameterValues: [2*i, 2*i+1])
    try s.execute()
    try s.reset()
    try s.clearBindings()
}
```

### Fetch Data

The closure passed to `execute()` will be called with each result row.

```swift
try db.execute(sql: "SELECT * FROM t1;") { row in
    let x: Int = try row.value(at: 0)
    let y: Int? = try row.value(at: 1)
}
```

`row` is a `Row` instance.

### Perform a Transaction

```swift
try db.transaction { db in
    // do something with `db`
    return .commit
}
```

Database transactions may also be performed asynchronously using `DatabaseQueue`.

```swift
dbQ.asyncTransaction { db in
    // do something with `db`
    return .commit
}
```

### Custom SQL Functions

```swift
let rot13Mapping: [Character: Character] = [
    "A": "N", "B": "O", "C": "P", "D": "Q", "E": "R", "F": "S", "G": "T", "H": "U", "I": "V", "J": "W", "K": "X", "L": "Y", "M": "Z",
    "N": "A", "O": "B", "P": "C", "Q": "D", "R": "E", "S": "F", "T": "G", "U": "H", "V": "I", "W": "J", "X": "K", "Y": "L", "Z": "M",
    "a": "n", "b": "o", "c": "p", "d": "q", "e": "r", "f": "s", "g": "t", "h": "u", "i": "v", "j": "w", "k": "x", "l": "y", "m": "z",
    "n": "a", "o": "b", "p": "c", "q": "d", "r": "e", "s": "f", "t": "g", "u": "h", "v": "i", "w": "j", "x": "k", "y": "l", "z": "m"]

try db.addFunction("rot13", arity: 1) { values in
    let value = values.first.unsafelyUnwrapped
    switch value {
        case .text(let s):
            return .text(String(s.map { rot13Mapping[$0] ?? $0 }))
        default:
            return value
    }
}
```

`rot13()` can now be used just like any other [SQL function](https://www.sqlite.org/lang_corefunc.html).

```swift
let s = try db.prepare(sql: "INSERT INTO t1(a) VALUES (rot13(?));")
```

### Custom Collating Sequences

```swift
try db.addCollation("localized_compare", { (lhs, rhs) -> ComparisonResult in
    return lhs.localizedCompare(rhs)
})
```

`localized_compare` is now available as a [collating sequence](https://www.sqlite.org/c3ref/create_collation.html).

```swift
let s = try db.prepare(sql: "SELECT * FROM t1 ORDER BY a COLLATE localized_compare;")
```

## License

FeistyDB is released under the [MIT License](https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt).
