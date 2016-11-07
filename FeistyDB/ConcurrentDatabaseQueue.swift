/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

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
		guard let result: String = try statement.makeIterator().next()?.value(at: 0), result == "wal" else {
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
		guard let result: String = try statement.makeIterator().next()?.value(at: 0), result == "wal" else {
			throw DatabaseError("Could not set journaling mode to WAL")
		}
	}

	/// Submits an asynchronous read operation to the database queue.
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
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the transaction after `block` completes
	public func transaction(type: Database.TransactionType = .deferred, _ block: @escaping (_ database: Database, _ rollback: inout Bool) -> (Void)) {
		queue.async(flags: .barrier) {
			do {
				try self.database.begin(type: type)
				var rollback = false
				block(self.database, &rollback)
				try rollback ? self.database.rollback() : self.database.commit()
			}
			catch {}
		}
	}

	/// Performs a synchronous read/write transaction on the database.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the transaction after `block` completes
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed
	///
	/// - returns: The value returned by `block`
	public func transaction_sync<T>(type: Database.TransactionType = .deferred, _ block: (_ database: Database, _ rollback: inout Bool) throws -> (T)) rethrows -> T {
		return try queue.sync(flags: .barrier) {
			try database.begin(type: type)
			var rollback = false
			let result = try block(database, &rollback)
			try rollback ? database.rollback() : database.commit()
			return result
		}
	}

	/// Submits an asynchronous read/write savepoint to the database queue.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	public func savepoint(block: @escaping (_ database: Database, _ rollback: inout Bool) -> (Void)) {
		queue.async(flags: .barrier) {
			do {
				let savepointUUID = UUID().uuidString
				try self.database.begin(savepoint: savepointUUID)
				var rollback = false
				block(self.database, &rollback)
				try rollback ? self.database.rollback(to: savepointUUID) : self.database.release(savepoint: savepointUUID)
			}
			catch {}
		}
	}

	/// Performs a synchronous read/write savepoint on the database.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released
	///
	/// - returns: The value returned by `block`
	public func savepoint_sync<T>(block: (_ database: Database, _ rollback: inout Bool) throws -> (T)) rethrows -> T {
		return try queue.sync(flags: .barrier) {
			let savepointUUID = UUID().uuidString
			try database.begin(savepoint: savepointUUID)
			var rollback = false
			let result = try block(database, &rollback)
			try rollback ? database.rollback(to: savepointUUID) : database.release(savepoint: savepointUUID)
			return result
		}
	}
}
