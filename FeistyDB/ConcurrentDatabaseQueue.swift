//
// Copyright (c) 2015 - 2018 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation
import os.log

/// A queue providing concurrent execution (multiple readers and one writer) of work items on a database.
///
/// A concurrent database queue requires SQLite's [write-ahead logging](https://sqlite.org/wal.html) 
/// journaling mode to support concurrent reading and writing.
///
/// Database operations may be submitted for synchronous or asynchronous execution.
///
/// ```swift
/// let dbQueue = ConcurrentDatabaseQueue(url: URL(fileURLWithPath: "/tmp/db.sqlite"))
/// dbQueue.read { db in
///     // Read something from `db` asynchronously
/// }
/// ```
public final class ConcurrentDatabaseQueue {
	/// The underlying database
	private let database: Database
	/// The dispatch queue used for concurrent access
	private let queue = DispatchQueue(label: "com.feisty-dog.FeistyDB.ConcurrentDatabaseQueue", attributes: .concurrent)

	/// Creates a database queue for concurrent access to a database from a file.
	///
	/// - parameter url: The location of the SQLite database
	///
	/// - throws: An error if the database could not be opened or WAL journaling mode could not be set
	public init(url: URL) throws {
		self.database = try Database(url: url)

		// Set WAL mode
		let statement = try database.prepare(sql: "PRAGMA journal_mode = WAL;")
		guard let result: String = try statement.front(), result == "wal" else {
			throw DatabaseError("Could not set journaling mode to WAL")
		}
	}

	/// Creates a database queue for concurrent access to an existing database.
	///
	/// - attention: The database queue takes ownership of `database`.  The result of further use of `database` is undefined.
	///
	/// - parameter database: The database
	///
	/// - throws: An error if WAL journaling mode could not be set
	public init(database: Database) throws {
		self.database = database

		// Set WAL mode
		let statement = try database.prepare(sql: "PRAGMA journal_mode = WAL;")
		guard let result: String = try statement.front(), result == "wal" else {
			throw DatabaseError("Could not set journaling mode to WAL")
		}
	}

	/// Returns a compiled SQL statement.
	///
	/// Use this instead of `Database.prepare(sql:)` because it
	/// serializes operations that can change the database, which
	/// may not occur on different threads.
	///
	/// - parameter sql: The SQL statement to compile
	///
	/// - throws: An error if `sql` could not be compiled
	///
	/// - returns: A compiled SQL statement
	public func prepare(sql: String) throws -> Statement {
		return try queue.sync(flags: .barrier) {
			return try database.prepare(sql: sql)
		}
	}

	/// Compiles an SQL statement and submits an asynchronous read operation to the database queue.
	///
	/// - parameter sql: The SQL statement to compile
	///
	/// - important: Only read operations may be performed in `block`.  Actions
	/// that modify the underlying database, including `Database.prepare(sql:)`,
	/// are not allowed.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter statement: A compiled version of `sql` used within `block`
	public func read(sql: String, _ block: @escaping (_ statement: Statement) -> (Void)) throws {
		let statement = try prepare(sql: sql)

		#if DEBUG
			guard statement.isReadOnly else {
				throw DatabaseError("Statement is not read-only")
			}
		#endif

		return queue.async {
			block(statement)
		}
	}

	/// Submits an asynchronous read operation to the database queue.
	///
	/// - important: Only read operations may be performed in `block`.  Actions
	/// that modify the underlying database, including `Database.prepare(sql:)`,
	/// are not allowed.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	public func read(block: @escaping (_ database: Database) -> (Void)) {
		return queue.async {
			block(self.database)
		}
	}

	/// Performs a synchronous read operation on the database.
	///
	/// - important: Only read operations may be performed in `block`.  Actions
	/// that modify the underlying database, including `Database.prepare(sql:)`,
	/// are not allowed.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	///
	/// - throws: Any error thrown in `block`
	///
	/// - returns: The value returned by `block`
	public func read_sync<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		return try queue.sync {
			return try block(self.database)
		}
	}

	/// Submits an asynchronous write operation to the database queue.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	public func write(block: @escaping (_ database: Database) -> (Void)) {
		return queue.async(flags: .barrier) {
			block(self.database)
		}
	}

	/// Performs a synchronous write operation on the database.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - throws: Any error thrown in `block`
	/// - returns: The value returned by `block`
	public func write_sync<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		return try queue.sync(flags: .barrier) {
			return try block(self.database)
		}
	}

	/// Submits an asynchronous read/write transaction to the database queue.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	public func transaction(type: Database.TransactionType = .deferred, _ block: @escaping Database.TransactionBlock) {
		queue.async(flags: .barrier) {
			do {
				try self.database.transaction(type: type, block)
			}
			catch let error {
				os_log("Error performing database transaction: %{public}@", type: .error, error.localizedDescription);
			}
		}
	}

	/// Performs a synchronous read/write transaction on the database.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed
	///
	/// - note: If `block` throws an error the transaction will be rolled back and the error will be re-thrown
	/// - note: If an error occurs committing the transaction a rollback will be attempted and the error will be re-thrown
	public func transaction_sync(type: Database.TransactionType = .deferred, _ block: Database.TransactionBlock) throws {
		try queue.sync(flags: .barrier) {
			try database.transaction(type: type, block)
		}
	}

	/// Submits an asynchronous read/write savepoint transaction to the database queue.
	///
	/// - parameter block: A closure performing the database operation
	public func savepoint(block: @escaping Database.SavepointBlock) {
		queue.async(flags: .barrier) {
			do {
				try self.database.savepoint(block: block)
			}
			catch let error {
				os_log("Error performing database savepoint: %{public}@", type: .error, error.localizedDescription);
			}
		}
	}

	/// Performs a synchronous read/write savepoint transaction on the database.
	///
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released
	///
	/// - note: If `block` throws an error the savepoint will be rolled back and the error will be re-thrown
	/// - note: If an error occurs releasing the savepoint a rollback will be attempted and the error will be re-thrown
	public func savepoint_sync(block: Database.SavepointBlock) throws {
		try queue.sync(flags: .barrier) {
			try database.savepoint(block: block)
		}
	}

	/// Perform a write-ahead log checkpoint on the database.
	///
	/// - parameter type: The type of checkpoint to perform
	///
	/// - throws: An error if the checkpoint failed
	public func checkpoint(_ type: Database.WALCheckpointType = .passive) throws {
		try queue.sync(flags: .barrier) {
			try database.walCheckpoint(type: type)
		}
	}
}
