//
// Copyright (c) 2018 - 2019 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// A queue providing serialized execution of read operations on a database.
///
/// Normally read queues are used for concurrent read access to databases using WAL mode.
///
/// A database read queue manages the execution of read-only database operations to
/// ensure they occur one at a time in FIFO order.  This provides thread-safe
/// database access.
///
/// Database read operations may be submitted for synchronous or asynchronous execution.
///
/// It is possible to maintain a consistent snapshot of a database using read
/// transactions. Changes committed to a database are not visible within a read transaction
/// until the transaction is updated or restarted.
///
/// The interface is similar to `DispatchQueue` and a dispatch queue is used
/// internally for work item management.
public final class DatabaseReadQueue {
	/// The underlying database
	let database: Database
	/// The dispatch queue used to serialize access to the underlying database connection
	public let queue: DispatchQueue

	/// Creates a database read queue for serialized read access to a database from a file.
	///
	/// - parameter url: The location of the SQLite database
	/// - parameter qos: The quality of service class for the work performed by the database queue
	///
	/// - throws: An error if the database could not be opened
	public init(url: URL, qos: DispatchQoS = .default) throws {
		self.database = try Database(readingFrom: url)
		self.queue = DispatchQueue(label: "com.feisty-dog.FeistyDB.DatabaseReadQueue", qos: qos)
	}

	/// Creates a database read queue for serialized read access to an existing database.
	///
	/// - attention: The database queue takes ownership of `database`.  The result of further use of `database` is undefined.
	///
	/// - parameter database: The database to be serialized
	/// - parameter qos: The quality of service class for the work performed by the database queue
	public init(database: Database, qos: DispatchQoS = .default) {
		self.database = database
		self.queue = DispatchQueue(label: "com.feisty-dog.FeistyDB.DatabaseReadQueue", qos: qos)
	}

	/// Begins a long-running read transaction on the database.
	///
	/// - throws: An error if the transaction could not be started
	public func beginReadTransaction() throws {
		try sync { db in
			try db.beginReadTransaction()
		}
	}

	/// Ends a long-running read transaction on the database.
	///
	/// - throws: An error if the transaction could not be rolled back
	public func endReadTransaction() throws {
		try sync { db in
			try db.endReadTransaction()
		}
	}

	/// Updates a long-running read transaction to make the latest database changes visible.
	///
	/// If there is an active read transaction it is ended before beginning a new read transaction.
	///
	/// - throws: An error if the transaction could not be rolled back or started
	public func updateReadTransaction() throws {
		try sync { db in
			try db.updateReadTransaction()
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
	public func sync<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		return try queue.sync {
			return try block(self.database)
		}
	}

	/// Submits an asynchronous read operation to the database queue.
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
}

extension DatabaseReadQueue {
	/// Creates a database read queue for serialized read access to a database from the file corresponding to the database *main* on a write queue.
	///
	/// - note: The QoS for the database queue is set to the QoS of `writeQueue`
	///
	/// - parameter writeQueue: A database queue for the SQLite database
	///
	/// - throws: An error if the database could not be opened
	public convenience init(writeQueue: DatabaseQueue) throws {
		try self.init(writeQueue: writeQueue, qos: writeQueue.queue.qos)
	}

	/// Creates a database read queue for serialized read access to a database from the file corresponding to the database *main* on a write queue.
	///
	/// - parameter writeQueue: A database queue for the SQLite database
	/// - parameter qos: The quality of service class for the work performed by the database queue
	///
	/// - throws: An error if the database could not be opened
	public convenience init(writeQueue: DatabaseQueue, qos: DispatchQoS) throws {
		let url = try writeQueue.sync { db in
			return try db.url(forDatabase: "main")
		}
		try self.init(url: url, qos: qos)
	}
}

extension Database {
	/// Begins a long-running read transaction on the database.
	///
	/// This is equivalent to the SQL `BEGIN DEFERRED TRANSACTION;`
	///
	/// - throws: An error if the transaction could not be started
	public func beginReadTransaction() throws {
		try begin(type: .deferred)
	}

	/// Ends a long-running read transaction on the database.
	///
	/// This is equivalent to the SQL `ROLLBACK;`
	///
	/// - throws: An error if the transaction could not be rolled back
	public func endReadTransaction() throws {
		try rollback()
	}

	/// Updates a long-running read transaction to make the latest database changes visible.
	///
	/// If there is an active read transaction it is ended before beginning a new read transaction.
	///
	/// - throws: An error if the transaction could not be started
	public func updateReadTransaction() throws {
		if !isInAutocommitMode {
			try rollback()
		}
		try beginReadTransaction()
	}
}
