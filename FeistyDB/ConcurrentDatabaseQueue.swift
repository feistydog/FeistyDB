/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A class providing concurrent access (multiple readers and one writer) to a database
public final class ConcurrentDatabaseQueue {
	/// The underlying database
	private var database: Database
	/// The dispatch queue used for concurrent access
	private let queue = DispatchQueue(label: "com.feisty-dog.FDDatabase.ConcurrentDatabaseQueue", attributes: .concurrent)

	/// Initialize the queue for concurrent access to a database
	///
	/// - parameter url: The URL of the database to access
	/// - throws: `DatabaseError`
	public init(url: URL) throws {
		self.database = try Database(url: url)

		// Set WAL mode
		let statement = try database.prepare(sql: "PRAGMA journal_mode = WAL;")
		guard let result: String = statement.results().makeIterator().next()?.column(0), result == "wal" else {
			throw DatabaseError.sqliteError("Could not set journaling mode to WAL")
		}
	}

	/// Initialize the queue for concurrent access to a database
	///
	/// **The queue takes ownership of the passed-in database**
	///
	/// - parameter database: The database to be serialized
	/// - throws: `DatabaseError`
	public init(database: Database) throws {
		self.database = database

		// Set WAL mode
		let statement = try database.prepare(sql: "PRAGMA journal_mode = WAL;")
		guard let result: String = statement.results().makeIterator().next()?.column(0), result == "wal" else {
			throw DatabaseError.sqliteError("Could not set journaling mode to WAL")
		}
	}

	/// Perform an asynchronous read operation on the database
	///
	/// - parameter block: The block performing the operation
	/// - parameter database: The `Database` used for database access
	public func read(block: @escaping (_ database: Database) -> (Void)) {
		return queue.async {
			block(self.database)
		}
	}

	/// Perform a synchronous read operation on the database
	///
	/// - parameter block: The block performing the operation
	/// - parameter database: The `Database` used for database access
	/// - throws: Any error thrown in `block`
	/// - returns: The value returned by `block`
	public func read_sync<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		return try queue.sync {
			return try block(self.database)
		}
	}

	/// Perform an asynchronous write operation on the database
	///
	/// - parameter block: The block performing the operation
	/// - parameter database: The `Database` used for database access
	public func write(block: @escaping (_ database: Database) -> (Void)) {
		return queue.async(flags: .barrier) {
			block(self.database)
		}
	}

	/// Perform a synchronous write operation on the database
	///
	/// - parameter block: The block performing the operation
	/// - parameter database: The `Database` used for database access
	/// - throws: Any error thrown in `block`
	/// - returns: The value returned by `block`
	public func write_sync<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		return try queue.sync(flags: .barrier) {
			return try block(self.database)
		}
	}

	/// Perform an asynchronous read/write transaction on the database
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: The block performing the read/write
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: Whether to rollback the transaction after `block` completes
	public func transaction(type: TransactionType = .deferred, _ block: @escaping (_ database: Database, _ rollback: inout Bool) -> (Void)) {
		queue.async(flags: .barrier) {
			try? self.database.transaction(type:type, block)
		}
	}

	/// Perform a synchronous read/write transaction on the database
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: The block performing the read/write
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: If set to `true` by `block` the transaction will be rolled back
	/// - throws: Any error thrown in `block`
	/// - returns: The value returned by `block`
	public func transaction_sync<T>(type: TransactionType = .deferred, _ block: (_ database: Database, _ rollback: inout Bool) throws -> (T)) rethrows -> T {
		return try queue.sync(flags: .barrier) {
			try database.transaction(type: type, block)
		}
	}

	/// Perform an asynchronous savepoint on the database
	///
	/// - parameter block: The block performing the read/write
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	public func savepoint(block: @escaping (_ database: Database, _ rollback: inout Bool) -> (Void)) {
		queue.async(flags: .barrier) {
			try? self.database.savepoint(block)
		}
	}

	/// Perform a synchronous savepoint on the database
	///
	/// - parameter block: The block performing the read/write
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	/// - returns: The value returned by `block`
	public func savepoint_sync<T>(block: (_ database: Database, _ rollback: inout Bool) throws -> (T)) rethrows -> T {
		return try queue.sync(flags: .barrier) {
			try self.database.savepoint(block)
		}
	}
}
