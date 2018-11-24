//
// Copyright (c) 2015 - 2018 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation
import os.log

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
	/// The dispatch queue used to serialize access to the underlying database connection
	public let queue: DispatchQueue

	/// Creates a database queue for serialized access to an in-memory database.
	///
	/// - throws: An error if the database could not be created
	public init(qos: DispatchQoS = .default) throws {
		self.database = try Database()
		self.queue = DispatchQueue(label: "com.feisty-dog.FeistyDB.DatabaseQueue", qos: qos)
	}

	/// Creates a database queue for serialized access to a database from a file.
	///
	/// - parameter url: The location of the SQLite database
	///
	/// - throws: An error if the database could not be opened
	public init(url: URL, qos: DispatchQoS = .default) throws {
		self.database = try Database(url: url)
		self.queue = DispatchQueue(label: "com.feisty-dog.FeistyDB.DatabaseQueue", qos: qos)
	}

	/// Creates a database queue for serialized access to an existing database.
	///
	/// - attention: The database queue takes ownership of `database`.  The result of further use of `database` is undefined.
	///
	/// - parameter database: The database to be serialized
	public init(database: Database, qos: DispatchQoS = .default) {
		self.database = database
		self.queue = DispatchQueue(label: "com.feisty-dog.FeistyDB.DatabaseQueue", qos: qos)
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
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`
	/// - parameter qos: The quality of service for `block`
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	public func async(group: DispatchGroup? = nil, qos: DispatchQoS = .unspecified, block: @escaping (_ database: Database) -> (Void)) {
		queue.async(group: group, qos: qos) {
			block(self.database)
		}
	}

	/// Performs a synchronous transaction on the database.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed
	///
	/// - note: If `block` throws an error the transaction will be rolled back and the error will be re-thrown
	/// - note: If an error occurs committing the transaction a rollback will be attempted and the error will be re-thrown
	public func transaction(type: Database.TransactionType = .deferred, _ block: Database.TransactionBlock) throws {
		try queue.sync {
			try database.transaction(type: type, block)
		}
	}

	/// Submits an asynchronous transaction to the database queue.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`
	/// - parameter qos: The quality of service for `block`
	/// - parameter block: A closure performing the database operation
	public func transaction_async(type: Database.TransactionType = .deferred, group: DispatchGroup? = nil, qos: DispatchQoS = .unspecified, _ block: @escaping Database.TransactionBlock) {
		queue.async(group: group, qos: qos) {
			do {
				try self.database.transaction(type: type, block)
			}
			catch let error {
				os_log("Error performing database transaction: %{public}@", type: .info, error.localizedDescription);
			}
		}
	}

	/// Performs a synchronous savepoint transaction on the database.
	///
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released
	///
	/// - note: If `block` throws an error the savepoint will be rolled back and the error will be re-thrown
	/// - note: If an error occurs releasing the savepoint a rollback will be attempted and the error will be re-thrown
	public func savepoint(block: Database.SavepointBlock) throws {
		try queue.sync {
			try database.savepoint(block: block)
		}
	}

	/// Submits an asynchronous savepoint transaction to the database queue.
	///
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`
	/// - parameter qos: The quality of service for `block`
	/// - parameter block: A closure performing the database operation
	public func savepoint_async(group: DispatchGroup? = nil, qos: DispatchQoS = .unspecified, block: @escaping Database.SavepointBlock) {
		queue.async(group: group, qos: qos) {
			do {
				try self.database.savepoint(block: block)
			}
			catch let error {
				os_log("Error performing database savepoint: %{public}@", type: .info, error.localizedDescription);
			}
		}
	}
}
