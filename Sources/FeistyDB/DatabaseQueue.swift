//
// Copyright (c) 2015 - 2026 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Dispatch
import Foundation
import CSQLite

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
/// let dbQueue = try DatabaseQueue()
/// try dbQueue.sync { db in
///     // Do something with `db`
/// }
/// ```
///
/// A database queue also supports transactions and savepoints:
///
/// ```swift
/// let result = dbQueue.transaction { db in
///     // All database operations here are contained within a transaction
///     return .commit
/// }
/// ```
public final class DatabaseQueue: @unchecked Sendable {
	/// The underlying database
	let database: Database
	/// The dispatch queue used to serialize access to the underlying database connection
	public let queue: DispatchQueue

	/// Creates a database queue for serialized access to an in-memory database.
	///
	/// - parameter label: The label to attach to the queue
	/// - parameter qos: The quality of service class for the work performed by the database queue
	/// - parameter target: The target dispatch queue on which to execute blocks
	///
	/// - throws: An error if the database could not be created
	public init(label: String, qos: DispatchQoS = .default, target: DispatchQueue? = nil) throws {
		self.database = try Database()
		self.queue = DispatchQueue(label: label, qos: qos, target: target)
	}

	/// Creates a database queue for serialized access to a database from a file.
	///
	/// - parameter url: The location of the SQLite database
	/// - parameter label: The label to attach to the queue
	/// - parameter qos: The quality of service class for the work performed by the database queue
	/// - parameter target: The target dispatch queue on which to execute blocks
	///
	/// - throws: An error if the database could not be opened
	public init(url: URL, label: String, qos: DispatchQoS = .default, target: DispatchQueue? = nil) throws {
		self.database = try Database(url: url)
		self.queue = DispatchQueue(label: label, qos: qos, target: target)
	}

	/// Creates a database queue for serialized access to an existing database.
	///
	/// - attention: The database queue takes ownership of `database`.  The result of further use of `database` is undefined.
	///
	/// - parameter database: The database to be serialized
	/// - parameter label: The label to attach to the queue
	/// - parameter qos: The quality of service class for the work performed by the database queue
	/// - parameter target: The target dispatch queue on which to execute blocks
	public init(database: sending Database, label: String, qos: DispatchQoS = .default, target: DispatchQueue? = nil) {
		self.database = database
		self.queue = DispatchQueue(label: label, qos: qos, target: target)
	}

	/// A block called with a database instance.
	///
	/// - parameter database: A `Database` used for database access within the block
	public typealias DatabaseBlock<T> = @Sendable (_ database: Database) throws -> sending T

	/// Performs a synchronous operation on the database.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	///
	/// - throws: Any error thrown in `block`
	///
	/// - returns: The value returned by `block`
	public func sync<T>(block: DatabaseBlock<T>) rethrows -> sending T {
		try queue.sync {
			try block(self.database)
		}
	}

	/// A block called with the result of an asynchronous database operation.
	///
	/// - parameter result: A `Result` object containing the result of the operation.
	public typealias CompletionHandler<T> = @Sendable (_ result: Result<T, Error>) -> Void

	/// Submits an asynchronous operation to the database queue.
	///
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`
	/// - parameter qos: The quality of service for `block`
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	/// - parameter completion: A closure called with the result of the operation.
	public func async<T: Sendable>(group: DispatchGroup? = nil, qos: DispatchQoS = .default, block: @escaping DatabaseBlock<T>, completion: @escaping CompletionHandler<T>) {
		queue.async(group: group, qos: qos) {
			do {
				let result = try block(self.database)
				completion(.success(result))
			} catch let error {
				completion(.failure(error))
			}
		}
	}

	/// Performs a synchronous operation on the database.
	///
	/// - parameter block: A closure performing the database operation
	/// - parameter database: A `Database` used for database access within `block`
	///
	/// - throws: Any error thrown in `block`
	///
	/// - returns: The value returned by `block`
	@available(macOS 10.14, iOS 12.0, tvOS 12.0, watchOS 5.0, *)
	public func asyncAndWait<T>(block: DatabaseBlock<T>) rethrows -> sending T {
		try queue.asyncAndWait {
			try block(self.database)
		}
	}

	/// A series of database actions grouped into a transaction
	///
	/// - parameter database: A `Database` used for database access within the block
	///
	/// - returns: `.commit` if the transaction should be committed or `.rollback` if the transaction should be rolled back
	public typealias TransactionBlock = @Sendable (_ database: Database) throws -> Database.TransactionCompletion

	/// Performs a synchronous transaction on the database.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	/// - parameter completion: A closure called with the result of the operation.
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed
	///
	/// - returns: The result of the transaction.
	///
	/// - note: If `block` throws an error the transaction will be rolled back and the error will be re-thrown
	/// - note: If an error occurs committing the transaction a rollback will be attempted and the error will be re-thrown
	@discardableResult public func transaction(type: Database.TransactionType = .deferred, _ block: TransactionBlock) throws -> Database.TransactionCompletion {
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
	/// - parameter completion: A closure called with the result of the transaction.
	public func asyncTransaction(type: Database.TransactionType = .deferred, group: DispatchGroup? = nil, qos: DispatchQoS = .default, _ block: @escaping TransactionBlock, completion: @escaping CompletionHandler<Database.TransactionCompletion>) {
		queue.async(group: group, qos: qos) {
			do {
				let result = try self.database.transaction(type: type, block)
				completion(.success(result))
			} catch let error {
				completion(.failure(error))
			}
		}
	}

	/// A series of database actions grouped into a savepoint transaction
	///
	/// - parameter database: A `Database` used for database access within the block
	///
	/// - returns: `.release` if the savepoint should be released or `.rollback` if the savepoint should be rolled back
	public typealias SavepointBlock = @Sendable (_ database: Database) throws -> Database.SavepointCompletion

	/// Performs a synchronous savepoint transaction on the database.
	///
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released
	///
	/// - returns: The result of the savepoint transaction.
	///
	/// - note: If `block` throws an error the savepoint will be rolled back and the error will be re-thrown
	/// - note: If an error occurs releasing the savepoint a rollback will be attempted and the error will be re-thrown
	@discardableResult public func savepoint(block: SavepointBlock) throws -> Database.SavepointCompletion {
		try queue.sync {
			try database.savepoint(block: block)
		}
	}

	/// Submits an asynchronous savepoint transaction to the database queue.
	///
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`
	/// - parameter qos: The quality of service for `block`
	/// - parameter block: A closure performing the database operation
	/// - parameter completion: A closure called with the result of the savepoint.
	public func asyncSavepoint(group: DispatchGroup? = nil, qos: DispatchQoS = .default, block: @escaping SavepointBlock, completion: @escaping CompletionHandler<Database.SavepointCompletion>) {
		queue.async(group: group, qos: qos) {
			do {
				let result = try self.database.savepoint(block: block)
				completion(.success(result))
			} catch let error {
				completion(.failure(error))
			}
		}
	}

	/// Performs an unsafe database operation.
	///
	/// - warning: To ensure thread safety all access to `database` within `block` **must** use `queue`.
	/// - attention: Use of this function should be avoided whenever possible.
	///
	/// - parameter block: A closure performing the database operation.
	/// - parameter database: The `Database` object.
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func withUnsafeDatabase<T>(block: (_ database: Database) throws -> (T)) rethrows -> T {
		try block(database)
	}
}

@available(macOS 10.15.0, iOS 13.0.0, tvOS 13.0.0, watchOS 6.0.0, *)
extension TaskPriority {
	/// Returns the approximate equivalent dispatch quality-of-service for `self`
	var dispatchQoS: DispatchQoS {
		switch self {
		case .high:
			return .userInitiated
		case .medium:
			return .default
		case .low:
			return .utility
		case .background:
			return .background
		default:
			return .default
		}
	}
}

@available(macOS 10.15.0, iOS 13.0.0, tvOS 13.0.0, watchOS 6.0.0, *)
extension DatabaseQueue {
	/// Performs an asynchronous database operation.
	///
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func perform<T>(block: @escaping DatabaseBlock<T>) async throws -> sending T {
		try await withCheckedThrowingContinuation { continuation in
			queue.async(qos: Task.currentPriority.dispatchQoS) {
				do {
					continuation.resume(returning: try block(self.database))
				} catch {
					continuation.resume(throwing: error)
				}
			}
		}
	}

	/// Performs an asynchronous transaction on the database.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The result of the transaction.
	@discardableResult public func performTransaction(type: Database.TransactionType = .deferred, _ block: @escaping TransactionBlock) async throws -> Database.TransactionCompletion {
		try await perform { try $0.transaction(type: type, block) }
	}

	/// Performs an asynchronous savepoint transaction on the database.
	///
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The result of the savepoint.
	@discardableResult public func performSavepoint(block: @escaping SavepointBlock) async throws -> Database.SavepointCompletion {
		try await perform { try $0.savepoint(block: block) }
	}
}
