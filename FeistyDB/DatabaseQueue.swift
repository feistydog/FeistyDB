//
// Copyright (c) 2015 - 2017 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// A queue providing serialized execution of work items on a database.
///
/// A database queue manages the execution of database operations to ensure they
/// occur one at a time in FIFO order.  This provides thread-safe database access.
///
/// Database operations may be submitted for synchronous or asynchronous execution.
///
/// The interface is similar to `DispatchQueue` and a dispatch queue is used
/// internally for work item management.
///
/// ```swift
/// let dbQueue = DatabaseQueue()
/// dbQueue.sync { db in
///     // Do something with `db`
/// }
/// ```
///
/// A database queue also supports transactions and savepoints:
///
/// ```swift
/// dbQueue.transaction { db in
///     // All database operations here are contained within a transaction
/// }
/// ```
public final class DatabaseQueue {
	/// The underlying database
	private let database: Database
	/// The dispatch queue used to serialize access
	private let queue = DispatchQueue(label: "com.feisty-dog.FeistyDB.DatabaseQueue")

	/// Creates a database queue for serialized access to an in-memory database.
	///
	/// - throws: An error if the database could not be created
	public init() throws {
		self.database = try Database()
	}

	/// Creates a database queue for serialized access to a database from a file.
	///
	/// - parameter url: The location of the SQLite database
	///
	/// - throws: An error if the database could not be opened
	public init(url: URL) throws {
		self.database = try Database(url: url)
	}

	/// Creates a database queue for serialized access to an existing database.
	///
	/// - attention: The database queue takes ownership of `database`.  The result of further use of `database` is undefined.
	///
	/// - parameter database: The database to be serialized
	public init(database: Database) {
		self.database = database
	}

	/// Performs a synchronous operation on the database.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	///
	/// - throws: Any error thrown in `block`
	///
	/// - returns: The value returned by `block`
	public func sync<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		return try queue.sync {
			return try block(self.database)
		}
	}

	/// Submits an asynchronous operation to the database queue.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	public func async(block: @escaping (_ database: Database) -> (Void)) {
		queue.async {
			block(self.database)
		}
	}

	/// Performs a synchronous transaction on the database.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the transaction after `block` completes
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed
	///
	/// - returns: The value returned by `block`
	public func transaction<T>(type: Database.TransactionType = .deferred, _ block: (_ database: Database, _ rollback: inout Bool) throws -> (T)) rethrows -> T {
		return try queue.sync {
			try database.begin(type: type)
			var rollback = false
			let result = try block(database, &rollback)
			try rollback ? database.rollback() : database.commit()
			return result
		}
	}

	/// Submits an asynchronous transaction to the database queue.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the transaction after `block` completes
	public func transaction_async(type: Database.TransactionType = .deferred, _ block: @escaping (_ database: Database, _ rollback: inout Bool) -> (Void)) {
		queue.async {
			do {
				try self.database.begin(type: type)
				var rollback = false
				block(self.database, &rollback)
				try rollback ? self.database.rollback() : self.database.commit()
			}
			catch let error {
				#if DEBUG
					print(error)
				#endif
			}
		}
	}

	/// Performs a synchronous savepoint on the database.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released
	///
	/// - returns: The value returned by `block`
	public func savepoint<T>(block: (_ database: Database, _ rollback: inout Bool) throws -> (T)) rethrows -> T {
		return try queue.sync {
			let savepointUUID = UUID().uuidString
			try database.begin(savepoint: savepointUUID)
			var rollback = false
			let result = try block(database, &rollback)
			try rollback ? database.rollback(to: savepointUUID) : database.release(savepoint: savepointUUID)
			return result
		}
	}

	/// Submits an asynchronous savepoint to the database queue.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	public func savepoint_async(block: @escaping (_ database: Database, _ rollback: inout Bool) -> (Void)) {
		queue.async {
			do {
				let savepointUUID = UUID().uuidString
				try self.database.begin(savepoint: savepointUUID)
				var rollback = false
				block(self.database, &rollback)
				try rollback ? self.database.rollback(to: savepointUUID) : self.database.release(savepoint: savepointUUID)
			}
			catch let error {
				#if DEBUG
					print(error)
				#endif
			}
		}
	}
}
