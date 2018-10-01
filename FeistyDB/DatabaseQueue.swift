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
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`
	/// - parameter qos: The quality of service for `block`
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	public func async(group: DispatchGroup? = nil, qos: DispatchQoS = .unspecified, block: @escaping (_ database: Database) -> (Void)) {
		queue.async(group: group, qos: qos) {
			block(self.database)
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

	/// Performs a synchronous transaction on the database.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed
	///
	/// - note: If `block` throws an error the transaction will be rolled back and the error will be re-thrown
	/// - note: If an error occurs committing the transaction a rollback will be attempted and the error will be re-thrown
	public func transaction(type: Database.TransactionType = .deferred, _ block: TransactionBlock) throws {
		try queue.sync {
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

	/// Submits an asynchronous transaction to the database queue.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`
	/// - parameter qos: The quality of service for `block`
	/// - parameter block: A closure performing the database operation
	public func transaction_async(type: Database.TransactionType = .deferred, group: DispatchGroup? = nil, qos: DispatchQoS = .unspecified, _ block: @escaping TransactionBlock) {
		queue.async(group: group, qos: qos) {
			do {
				try self.database.begin(type: type)
			}
			catch let error {
				os_log("Error beginning transaction: %{public}@", type: .info, error.localizedDescription);
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
				os_log("Error during transaction: %{public}@", type: .info, error.localizedDescription);
				if !self.database.isInAutocommitMode {
					try? self.database.rollback()
				}
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

	/// Performs a synchronous savepoint on the database.
	///
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released
	///
	/// - note: If `block` throws an error the savepoint will be rolled back and the error will be re-thrown
	/// - note: If an error occurs releasing the savepoint a rollback will be attempted and the error will be re-thrown
	public func savepoint(block: SavepointBlock) throws {
		try queue.sync {
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

	/// Submits an asynchronous savepoint to the database queue.
	///
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`
	/// - parameter qos: The quality of service for `block`
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	public func savepoint_async(group: DispatchGroup? = nil, qos: DispatchQoS = .unspecified, block: @escaping SavepointBlock) {
		queue.async(group: group, qos: qos) {
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

	/// Performs a synchronous operation on the database.
	///
	/// - warning: This bypasses the internal dispatch queue and may result in
	/// unpredictability, data corruption, or a crash if used incorrectly.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	///
	/// - throws: Any error thrown in `block`
	///
	/// - returns: The value returned by `block`
	public func withUnsafeDatabase<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		return try block(self.database)
	}
}
