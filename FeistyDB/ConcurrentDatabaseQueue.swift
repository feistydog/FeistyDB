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

	/// Possible ways to complete a transaction
	public enum TransactionCompletion {
		/// The transaction should be committed
		case commit
		/// The transaction should be rolled back
		case rollback
	}

	/// A series of database actions grouped into a transaction
	///
	/// - parameter database: A `Database` used for database access within the block
	///
	/// - returns: `.commit` if the transaction should be committed or `.rollback` if the transaction should be rolled back
	public typealias TransactionBlock = (_ database: Database) throws -> TransactionCompletion

	/// Submits an asynchronous read/write transaction to the database queue.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the transaction after `block` completes
	public func transaction(type: Database.TransactionType = .deferred, _ block: @escaping TransactionBlock) {
		queue.async(flags: .barrier) {
			do {
				try self.database.begin(type: type)
			}
			catch let error {
				os_log("Error beginning transaction: %{public}@", type: .error, error.localizedDescription);
				return
			}

			do {
				let action = try block(self.database)
				switch action {
				case .commit:
					try self.database.commit()
				case .rollback:
					try self.database.rollback()
				}
			}
			catch let error {
				os_log("Error during transaction: %{public}@", type: .error, error.localizedDescription);
				if !self.database.isInAutocommitMode {
					try? self.database.rollback()
				}
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
	public func transaction_sync(type: Database.TransactionType = .deferred, _ block: TransactionBlock) throws {
		try queue.sync(flags: .barrier) {
			try database.begin(type: type)
			do {
				let action = try block(database)
				switch action {
				case .commit:
					try database.commit()
				case .rollback:
					try database.rollback()
				}
			}
			catch let error {
				if !database.isInAutocommitMode {
					try database.rollback()
				}
				throw error
			}
		}
	}

	/// Possible ways to complete a savepoint
	public enum SavepointCompletion {
		/// The savepoint should be released
		case release
		/// The savepoint should be rolled back
		case rollback
	}

	/// A series of database actions grouped into a savepoint
	///
	/// - parameter database: A `Database` used for database access within the block
	///
	/// - returns: `.release` if the savepoint should be released or `.rollback` if the savepoint should be rolled back
	public typealias SavepointBlock = (_ database: Database) throws -> SavepointCompletion

	/// Submits an asynchronous read/write savepoint to the database queue.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	public func savepoint(block: @escaping SavepointBlock) {
		queue.async(flags: .barrier) {
			let savepointUUID = UUID().uuidString

			do {
				try self.database.begin(savepoint: savepointUUID)
			}
			catch let error {
				os_log("Error beginning savepoint: %{public}@", type: .info, error.localizedDescription);
				return
			}

			do {
				let action = try block(self.database)
				switch action {
				case .release:
					try self.database.release(savepoint: savepointUUID)
				case .rollback:
					try self.database.rollback(to: savepointUUID)
				}
			}
			catch let error {
				os_log("Error during savepoint: %{public}@", type: .info, error.localizedDescription);
				try? self.database.rollback(to: savepointUUID)
			}
		}
	}

	/// Performs a synchronous read/write savepoint on the database.
	///
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released
	///
	/// - note: If `block` throws an error the savepoint will be rolled back and the error will be re-thrown
	/// - note: If an error occurs releasing the savepoint a rollback will be attempted and the error will be re-thrown
	public func savepoint_sync(block: SavepointBlock) throws {
		try queue.sync(flags: .barrier) {
			let savepointUUID = UUID().uuidString
			try database.begin(savepoint: savepointUUID)
			do {
				let action = try block(database)
				switch action {
				case .release:
					try database.release(savepoint: savepointUUID)
				case .rollback:
					try database.rollback(to: savepointUUID)
				}
			}
			catch let error {
				try? database.rollback(to: savepointUUID)
				throw error
			}
		}
	}
}
