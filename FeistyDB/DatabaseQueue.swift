/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A class providing serialized access to a database
public final class DatabaseQueue {
	/// The underlying database
	private let database: Database
	/// The dispatch queue used to serialize access
	private let queue = DispatchQueue(label: "com.feisty-dog.FDDatabase.DatabaseQueue")

	/// Initialize the queue for serialized access to an in-memory database
	///
	/// - throws: `DatabaseError`
	public init() throws {
		self.database = try Database()
	}

	/// Initialize the queue for serialized access to a database
	///
	/// - parameter url: The URL of the database to access
	/// - throws: `DatabaseError`
	public init(url: URL) throws {
		self.database = try Database(url: url)
	}

	/// Initialize the queue for serialized access to a database
	///
	/// **The queue takes ownership of the passed-in database**
	///
	/// - parameter database: The database to be serialized
	public init(database: Database) {
		self.database = database
	}

	/// Perform a synchronous operation on the database
	///
	/// - parameter block: The block performing the operation
	/// - parameter database: The `Database` used for database access
	/// - throws: Any error thrown in `block`
	/// - returns: The value returned by `block`
	public func sync<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		return try queue.sync {
			return try block(self.database)
		}
	}

	/// Perform an asynchronous operation on the database
	///
	/// - parameter block: The block performing the operation
	/// - parameter database: The `Database` used for database access
	public func async(block: @escaping (_ database: Database) -> (Void)) {
		queue.async {
			block(self.database)
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
	public func transaction<T>(type: TransactionType = .deferred, _ block: (_ database: Database, _ rollback: inout Bool) throws -> (T)) rethrows -> T {
		return try queue.sync {
			try database.transaction(type: type, block)
		}
	}

	/// Perform an asynchronous read/write transaction on the database
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: The block performing the read/write
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: Whether to rollback the transaction after `block` completes
	public func transaction_async(type: TransactionType = .deferred, _ block: @escaping (_ database: Database, _ rollback: inout Bool) -> (Void)) {
		queue.async {
			try? self.database.transaction(type:type, block)
		}
	}

	/// Perform a synchronous savepoint on the database
	///
	/// - parameter block: The block performing the read/write
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	/// - returns: The value returned by `block`
	public func savepoint<T>(block: (_ database: Database, _ rollback: inout Bool) throws -> (T)) rethrows -> T {
		return try queue.sync {
			try self.database.savepoint(block)
		}
	}

	/// Perform an asynchronous savepoint on the database
	///
	/// - parameter block: The block performing the read/write
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	public func savepoint_async(block: @escaping (_ database: Database, _ rollback: inout Bool) -> (Void)) {
		queue.async {
			try? self.database.savepoint(block)
		}
	}
}
