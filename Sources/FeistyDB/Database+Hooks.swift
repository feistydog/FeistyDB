//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation
import CSQLite

extension Database {
	/// A hook called when a database transaction is committed.
	///
	/// - returns: `true` if the commit operation is allowed to proceed, `false` otherwise
	///
	/// - seealso: [Commit And Rollback Notification Callbacks](https://www.sqlite.org/c3ref/commit_hook.html)
	public typealias CommitHook = () -> Bool

	/// Sets the hook called when a database transaction is committed.
	///
	/// - parameter commitHook: A closure called when a transaction is committed
	public func setCommitHook(_ block: @escaping CommitHook) {
		let context = UnsafeMutablePointer<CommitHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_commit_hook(db, { context in
			return context.unsafelyUnwrapped.assumingMemoryBound(to: CommitHook.self).pointee() ? 0 : 1
		}, context) {
			let oldContext = old.assumingMemoryBound(to: CommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the commit hook.
	public func removeCommitHook() {
		if let old = sqlite3_commit_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: CommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// A hook called when a database transaction is rolled back.
	///
	/// - seealso: [Commit And Rollback Notification Callbacks](https://www.sqlite.org/c3ref/commit_hook.html)
	public typealias RollbackHook = () -> Void

	/// Sets the hook called when a database transaction is rolled back.
	///
	/// - parameter rollbackHook: A closure called when a transaction is rolled back
	public func setRollbackHook(_ block: @escaping RollbackHook) {
		let context = UnsafeMutablePointer<RollbackHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_rollback_hook(db, { context in
			context.unsafelyUnwrapped.assumingMemoryBound(to: RollbackHook.self).pointee()
		}, context) {
			let oldContext = old.assumingMemoryBound(to: RollbackHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the rollback hook.
	public func removeRollbackHook() {
		if let old = sqlite3_rollback_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: RollbackHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}
}

extension Database {
	/// A hook called when a database transaction is committed in write-ahead log mode.
	///
	/// - parameter databaseName: The name of the database that was written to
	/// - parameter pageCount: The number of pages in the write-ahead log file
	///
	/// - returns: Normally `SQLITE_OK`
	///
	/// - seealso: [Write-Ahead Log Commit Hook](https://www.sqlite.org/c3ref/wal_hook.html)
	public typealias WALCommitHook = (_ databaseName: String, _ pageCount: Int) -> Int32

	/// Sets the hook called when a database transaction is committed in write-ahead log mode.
	///
	/// - parameter commitHook: A closure called when a transaction is committed
	public func setWALCommitHook(_ block: @escaping WALCommitHook) {
		let context = UnsafeMutablePointer<WALCommitHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_wal_hook(db, { context, db, db_name, pageCount in
			//			guard db == self.db else {
			//				fatalError("Unexpected database connection handle from sqlite3_wal_hook")
			//			}
			let database = String(utf8String: db_name.unsafelyUnwrapped).unsafelyUnwrapped
			return context.unsafelyUnwrapped.assumingMemoryBound(to: WALCommitHook.self).pointee(database, Int(pageCount))
		}, context) {
			let oldContext = old.assumingMemoryBound(to: WALCommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the write-ahead log commit hook.
	public func removeWALCommitHook() {
		if let old = sqlite3_wal_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: WALCommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}
}

extension Database {
	/// Possible types of database row changes.
	public enum	RowChangeType {
		/// A row was inserted
		case insert
		/// A row was deleted
		case delete
		/// A row was updated
		case update
	}

	/// A hook called when a row is inserted, deleted, or updated in a rowid table.
	///
	/// - parameter change: The type of change triggering the hook
	/// - parameter database: The name of the database containing the affected row
	/// - parameter table: The name of the table containing the affected row
	/// - parameter rowid: The `rowid` of the affected row
	///
	/// - seealso: [Commit And Rollback Notification Callbacks](https://www.sqlite.org/c3ref/commit_hook.html)
	/// - seealso: [Rowid Tables](https://www.sqlite.org/rowidtable.html)
	public typealias UpdateHook = (_ change: RowChangeType, _ database: String, _ table: String, _ rowid: Int64) -> Void

	/// Sets the hook called when a row is inserted, deleted, or updated in a rowid table.
	///
	/// - parameter updateHook: A closure called when a row is inserted, deleted, or updated
	public func setUpdateHook(_ block: @escaping UpdateHook) {
		let context = UnsafeMutablePointer<UpdateHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_update_hook(db, { context, op, db_name, table_name, rowid in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: UpdateHook.self)

			let changeType = RowChangeType(op)
			let database = String(utf8String: db_name.unsafelyUnwrapped).unsafelyUnwrapped
			let table = String(utf8String: table_name.unsafelyUnwrapped).unsafelyUnwrapped

			function_ptr.pointee(changeType, database, table, rowid)
		}, context) {
			let oldContext = old.assumingMemoryBound(to: UpdateHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the update hook.
	public func removeUpdateHook() {
		if let old = sqlite3_update_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: UpdateHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}
}

extension Database.RowChangeType {
	/// Convenience initializer for conversion of `SQLITE_` values
	///
	/// - parameter op: The second argument to the callback function passed to `sqlite3_update_hook()`
	init(_ op: Int32) {
		switch op {
		case SQLITE_INSERT: 	self = .insert
		case SQLITE_DELETE: 	self = .delete
		case SQLITE_UPDATE: 	self = .update
		default:				preconditionFailure("Unexpected row change type")
		}
	}
}

extension Database {
	/// A hook that may be called when an attempt is made to access a locked database table.
	///
	/// - parameter attempts: The number of times the busy handler has been called for the same event
	///
	/// - returns: `true` if the attempts to access the database should stop, `false` to continue
	///
	/// - seealso: [Register A Callback To Handle SQLITE_BUSY Errors](https://www.sqlite.org/c3ref/busy_handler.html)
	public typealias BusyHandler = (_ attempts: Int) -> Bool

	/// Sets a callback that may be invoked when an attempt is made to access a locked database table.
	///
	/// - parameter busyHandler: A closure called when an attempt is made to access a locked database table
	///
	/// - throws: An error if the busy handler couldn't be set
	public func setBusyHandler(_ block: @escaping BusyHandler) throws {
		if busyHandler == nil {
			busyHandler = UnsafeMutablePointer<BusyHandler>.allocate(capacity: 1)
		}
		else {
			busyHandler?.deinitialize(count: 1)
		}

		busyHandler?.initialize(to: block)

		guard sqlite3_busy_handler(db, { context, count in
			return context.unsafelyUnwrapped.assumingMemoryBound(to: BusyHandler.self).pointee(Int(count)) ? 0 : 1
		}, busyHandler) == SQLITE_OK else {
			busyHandler?.deinitialize(count: 1)
			busyHandler?.deallocate()
			busyHandler = nil
			throw DatabaseError("Error setting busy handler")
		}
	}

	/// Removes the busy handler.
	///
	/// - throws: An error if the busy handler couldn't be removed
	public func removeBusyHandler() throws {
		defer {
			busyHandler?.deinitialize(count: 1)
			busyHandler?.deallocate()
			busyHandler = nil
		}

		guard sqlite3_busy_handler(db, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error removing busy handler", takingDescriptionFromDatabase: db)
		}
	}

	/// Sets a busy handler that sleeps when an attempt is made to access a locked database table.
	///
	/// - parameter ms: The minimum time in milliseconds to sleep
	///
	/// - throws: An error if the busy timeout couldn't be set
	///
	/// - seealso: [Set A Busy Timeout](https://www.sqlite.org/c3ref/busy_timeout.html)
	public func setBusyTimeout(_ ms: Int) throws {
		defer {
			busyHandler?.deinitialize(count: 1)
			busyHandler?.deallocate()
			busyHandler = nil
		}

		guard sqlite3_busy_timeout(db, Int32(ms)) == SQLITE_OK else {
			throw DatabaseError("Error setting busy timeout")
		}
	}
}
