//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import os.log
import Foundation
import CSQLite

// C -> Swift Hacks
let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// An `sqlite3 *` object.
///
/// - seealso: [SQLite Database Connection Handle](https://sqlite.org/c3ref/sqlite3.html)
public typealias SQLiteDatabaseConnection = OpaquePointer

/// An [SQLite](https://sqlite.org) database.
///
/// A database supports SQL statement execution, transactions and savepoints, custom collation sequences, and custom SQL functions.
///
/// ```swift
/// let db = try Database()
/// try db.execute(sql: "create table t1(a);")
/// try db.execute(sql: "insert into t1 default values;")
/// let rowCount: Int = db.prepare(sql: "select count(*) from t1;").front()
/// print("t1 has \(rowCount) rows")
/// ```
public final class Database {
	/// The underlying `sqlite3 *` database
	let db: SQLiteDatabaseConnection

	/// The database's custom busy handler
	var busyHandler: UnsafeMutablePointer<BusyHandler>?

	/// Prepared statements
	var preparedStatements = [AnyHashable: Statement]()

	/// Creates a temporary database.
	///
	/// - parameter inMemory: Whether the temporary database should be created in-memory or on-disk
	///
	/// - throws: An error if the database could not be created
	public init(inMemory: Bool = true) throws {
		var db: SQLiteDatabaseConnection?
		let path = inMemory ? ":memory:" : ""
		let result = sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
		guard result == SQLITE_OK else {
			sqlite3_close(db)
			throw SQLiteError("Error creating temporary database", code: result)
		}

		self.db =  db!
	}

	/// Creates a read-only database from a file.
	///
	/// - parameter url: The location of the SQLite database
	///
	/// - throws: An error if the database could not be opened
	public init(readingFrom url: URL) throws {
		var db: SQLiteDatabaseConnection?
		try url.withUnsafeFileSystemRepresentation { path in
			let result = sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, nil)
			guard result == SQLITE_OK else {
				sqlite3_close(db)
				throw SQLiteError("Error opening database \(url)", code: result)
			}
		}

		self.db = db!
	}

	/// Creates a read-write database from a file.
	///
	/// - parameter url: The location of the SQLite database
	/// - parameter create: Whether to create the database if it doesn't exist
	///
	/// - throws: An error if the database could not be opened
	public init(url: URL, create: Bool = true) throws {
		var db: SQLiteDatabaseConnection?
		try url.withUnsafeFileSystemRepresentation { path in
			var flags = SQLITE_OPEN_READWRITE
			if create {
				flags |= SQLITE_OPEN_CREATE
			}

			let result = sqlite3_open_v2(path, &db, flags, nil)
			guard result == SQLITE_OK else {
				sqlite3_close(db)
				throw SQLiteError("Error opening database \(url)", code: result)
			}
		}

		self.db = db!
	}

	/// Creates a database from an existing `sqlite3 *` database connection handle.
	///
	/// - attention: The database takes ownership of `db`.  The result of further use of `db` is undefined.
	///
	/// - parameter db: An `sqlite3 *` database connection handle
	public init(rawSQLiteDatabase db: SQLiteDatabaseConnection) {
		self.db = db

		#if false
			sqlite3_trace_v2(db, UInt32(SQLITE_TRACE_PROFILE), { (T, C, P, X) -> Int32 in
				if T == UInt32(SQLITE_TRACE_PROFILE) {
					// P = sqlite3_stmt
					// X = int64_t*

					let stmt = SQLitePreparedStatement(P)
					let sql = String(cString: sqlite3_sql(stmt))

					if let nanos = X?.assumingMemoryBound(to: Int64.self).pointee {
						let seconds = Double(nanos) / Double(NSEC_PER_SEC)
						print("\"\(sql)\" took \(seconds) sec")
					}

				}

				return 0
			}, nil)
		#endif
	}

	deinit {
		preparedStatements.removeAll()
		sqlite3_close(db)
		busyHandler?.deinitialize(count: 1)
		busyHandler?.deallocate()
	}

	/// `true` if this database is read only, `false` otherwise
	public lazy var isReadOnly: Bool = {
		return sqlite3_db_readonly(self.db, nil) == 1
	}()

	/// The rowid of the most recent successful `INSERT` into a rowid table or virtual table
	public var lastInsertRowid: Int64? {
		let rowid = sqlite3_last_insert_rowid(db)
		return rowid != 0 ? rowid : nil
	}

	/// The number of rows modified, inserted or deleted by the most recently completed `INSERT`, `UPDATE` or `DELETE` statement
	public var changes: Int {
		return Int(sqlite3_changes(db))
	}

	/// The total number of rows inserted, modified or deleted by all `INSERT`, `UPDATE` or `DELETE` statements
	public var totalChanges: Int {
		return Int(sqlite3_total_changes(db))
	}

	/// Interrupts a long-running query.
	public func interrupt() {
		sqlite3_interrupt(db)
	}

	/// Returns the location of the file associated with database `name`.
	///
	/// - note: The main database file has the name *main*
	///
	/// - parameter name: The name of the attached database whose location is desired
	///
	/// - throws: An error if there is no attached database with the specified name, or if `name` is a temporary or in-memory database
	///
	/// - returns: The URL for the file associated with database `name`
	public func url(forDatabase name: String = "main") throws -> URL {
		guard let path = sqlite3_db_filename(self.db, name) else {
			throw DatabaseError("The database \(name) does not exist or is a temporary or in-memory database")
		}
		return URL(fileURLWithPath: String(cString: path))
	}

	/// Performs a low-level SQLite database operation.
	///
	/// **Use of this function should be avoided whenever possible**
	///
	/// - parameter block: The closure performing the database operation
	/// - parameter db: The raw `sqlite3 *` database connection handle
	///
	/// - throws: Any error thrown in `block`
	///
	/// - returns: The value returned by `block`
	public func withUnsafeRawSQLiteDatabase<T>(block: (_ db: SQLiteDatabaseConnection) throws -> (T)) rethrows -> T {
		return try block(self.db)
	}
}

extension Database {
	/// Returns a compiled SQL statement.
	///
	/// - parameter sql: The SQL statement to compile
	///
	/// - throws: An error if `sql` could not be compiled
	///
	/// - returns: A compiled SQL statement
	public func prepare(sql: String) throws -> Statement {
		return try Statement(database: self, sql: sql)
	}

	/// Executes an SQL statement.
	///
	/// This is a shortcut for `prepare(sql: sql).execute()`.
	///
	/// - requires: `sql` does not return any result rows
	///
	/// - parameter sql: The SQL statement to execute
	///
	/// - throws: An error if `sql` returned any result rows or could not be compiled or executed
	public func execute(sql: String) throws {
		try prepare(sql: sql).execute()
	}

	/// Executes an SQL statement and applies `block` to each result row.
	///
	/// This is a shortcut for `prepare(sql: sql).results(block)`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter block: A closure applied to each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: An error if `sql` could not be compiled or executed
	public func results(sql: String, _ block: ((_ row: Row) throws -> ())) throws {
		try prepare(sql: sql).results(block)
	}

	/// Executes one or more SQL statements and optionally applies `block` to each result row.
	///
	/// Multiple SQL statements are separated with a semicolon (`;`)
	///
	/// - parameter sql: The SQL statement or statements to execute
	/// - parameter block: An optional closure applied to each result row
	/// - parameter row: A dictionary of returned data keyed by column name
	///
	/// - throws: An error if `sql` could not be compiled or executed
	public func batch(sql: String, _ block: ((_ row: [String: String]) -> Void)? = nil) throws {
		var result: Int32
		var errmsg: UnsafeMutablePointer<Int8>?
		if let block = block {
			let context = Unmanaged.passRetained(block as AnyObject).toOpaque()
			result = sqlite3_exec(db, sql, { (context, count, raw_values, raw_names) -> Int32 in
				let values = UnsafeMutableBufferPointer<UnsafeMutablePointer<Int8>?>(start: raw_values, count: Int(count))
				let names = UnsafeMutableBufferPointer<UnsafeMutablePointer<Int8>?>(start: raw_names, count: Int(count))

				var row = [String: String]()
				for i in 0 ..< Int(count) {
					let raw_value = values[i].unsafelyUnwrapped
					let value = String(bytesNoCopy: raw_value, length: strlen(raw_value), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
					let raw_name = names[i].unsafelyUnwrapped
					let name = String(bytesNoCopy: raw_name, length: strlen(raw_name), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
					row[name] = value
				}

				let block = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(context.unsafelyUnwrapped)).takeRetainedValue() as! ([String: String]) -> Void
				block(row)

				return SQLITE_OK
			}, context, &errmsg)
		}
		else {
			result = sqlite3_exec(db, sql, nil, nil, &errmsg)
		}
		guard result == SQLITE_OK else {
			let details = errmsg != nil ? String(bytesNoCopy: errmsg.unsafelyUnwrapped, length: strlen(errmsg.unsafelyUnwrapped), encoding: .utf8, freeWhenDone: true).unsafelyUnwrapped : nil
			throw DatabaseError(message: "Error executing SQL", details: details)
		}
	}
}

extension Database {
	/// Possible database transaction types.
	///
	/// - seealso: [Transactions in SQLite](https://sqlite.org/lang_transaction.html)
	public enum TransactionType {
		/// A deferred transaction
		case deferred
		/// An immediate transaction
		case immediate
		/// An exclusive transaction
		case exclusive
	}

	/// Begins a database transaction.
	///
	/// - note: Database transactions may not be nested.
	///
	/// - parameter type: The type of transaction to initiate
	///
	/// - throws: An error if the transaction couldn't be started
	///
	/// - seealso: [BEGIN TRANSACTION](https://sqlite.org/lang_transaction.html)
	public func begin(type: TransactionType = .deferred) throws {
		let sql: String
		switch type {
		case .deferred:		sql = "BEGIN DEFERRED TRANSACTION;"
		case .immediate:	sql = "BEGIN IMMEDIATE TRANSACTION;"
		case .exclusive:	sql = "BEGIN EXCLUSIVE TRANSACTION;"
		}

		guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error beginning transaction", takingDescriptionFromDatabase: db)
		}
	}

	/// Rolls back the active database transaction.
	///
	/// - throws: An error if the transaction couldn't be rolled back or there is no active transaction
	public func rollback() throws {
		guard sqlite3_exec(db, "ROLLBACK;", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error rolling back", takingDescriptionFromDatabase: db)
		}
	}

	/// Commits the active database transaction.
	///
	/// - throws: An error if the transaction couldn't be committed or there is no active transaction
	public func commit() throws {
		guard sqlite3_exec(db, "COMMIT;", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error committing", takingDescriptionFromDatabase: db)
		}
	}

	/// `true` if this database is in autocommit mode, `false` otherwise
	///
	/// - seealso: [Test For Auto-Commit Mode](https://www.sqlite.org/c3ref/get_autocommit.html)
	public var isInAutocommitMode: Bool {
		return sqlite3_get_autocommit(db) != 0
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

	/// Performs a transaction on the database.
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed
	///
	/// - note: If `block` throws an error the transaction will be rolled back and the error will be re-thrown
	/// - note: If an error occurs committing the transaction a rollback will be attempted and the error will be re-thrown
	public func transaction(type: Database.TransactionType = .deferred, _ block: TransactionBlock) throws {
		try begin(type: type)
		do {
			let action = try block(self)
			switch action {
			case .commit:
				try commit()
			case .rollback:
				try rollback()
			}
		}
		catch let error {
			if !isInAutocommitMode {
				try rollback()
			}
			throw error
		}
	}
}

extension Database {
	/// Begins a database savepoint transaction.
	///
	/// - note: Savepoint transactions may be nested.
	///
	/// - parameter name: The name of the savepoint transaction
	///
	/// - throws: An error if the savepoint transaction couldn't be started
	///
	/// - seealso: [SAVEPOINT](https://sqlite.org/lang_savepoint.html)
	public func begin(savepoint name: String) throws {
		guard sqlite3_exec(db, "SAVEPOINT '\(name)';", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error creating savepoint", takingDescriptionFromDatabase: db)
		}
	}

	/// Rolls back a database savepoint transaction.
	///
	/// - parameter name: The name of the savepoint transaction
	///
	/// - throws: An error if the savepoint transaction couldn't be rolled back or doesn't exist
	public func rollback(to name: String) throws {
		guard sqlite3_exec(db, "ROLLBACK TO '\(name)';", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error rolling back savepoint", takingDescriptionFromDatabase: db)
		}
	}

	/// Releases (commits) a database savepoint transaction.
	///
	/// - note: Changes are not saved until the outermost transaction is released or committed.
	///
	/// - parameter name: The name of the savepoint transaction
	///
	/// - throws: An error if the savepoint transaction couldn't be committed or doesn't exist
	public func release(savepoint name: String) throws {
		guard sqlite3_exec(db, "RELEASE '\(name)';", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error releasing savepoint", takingDescriptionFromDatabase: db)
		}
	}

	/// Possible ways to complete a savepoint
	public enum SavepointCompletion {
		/// The savepoint should be released
		case release
		/// The savepoint should be rolled back
		case rollback
	}

	/// A series of database actions grouped into a savepoint transaction
	///
	/// - parameter database: A `Database` used for database access within the block
	///
	/// - returns: `.release` if the savepoint should be released or `.rollback` if the savepoint should be rolled back
	public typealias SavepointBlock = (_ database: Database) throws -> SavepointCompletion

	/// Performs a savepoint transaction on the database.
	///
	/// - parameter block: A closure performing the database operation
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released
	///
	/// - note: If `block` throws an error the savepoint will be rolled back and the error will be re-thrown
	/// - note: If an error occurs releasing the savepoint a rollback will be attempted and the error will be re-thrown
	public func savepoint(block: SavepointBlock) throws {
		let savepointUUID = UUID().uuidString
		try begin(savepoint: savepointUUID)
		do {
			let action = try block(self)
			switch action {
			case .release:
				try release(savepoint: savepointUUID)
			case .rollback:
				try rollback(to: savepointUUID)
			}
		}
		catch let error {
			try? rollback(to: savepointUUID)
			throw error
		}
	}
}

extension Database {
	/// Possible write-ahead log (WAL) checkpoint types.
	///
	/// - seealso: [Write-Ahead Logging](https://www.sqlite.org/wal.html)
	public enum WALCheckpointType {
		/// Checkpoint as many frames as possible without waiting for any database readers or writers to finish
		case passive
		/// Blocks until there is no writer and all readers are reading from the most recent database snapshot then checkpoints all frames
		case full
		/// Same as `WALCheckpointType.full` except after checkpointing it blocks until all readers are reading from the database file
		case restart
		/// Same as `WALCheckpointType.restart` except it also truncates the log file prior to a successful return
		case truncate
	}

	/// Perform a write-ahead log checkpoint on the database.
	///
	/// - note: Checkpoints are only valid for databases using write-ahead logging (WAL) mode.
	///
	/// - parameter type: The type of checkpoint to perform
	///
	/// - throws: An error if the checkpoint failed or if the database isn't in WAL mode
	///
	/// - seealso: [Checkpoint a database](https://www.sqlite.org/c3ref/wal_checkpoint_v2.html)
	/// - seealso: [PRAGMA wal_checkpoint](https://www.sqlite.org/pragma.html#pragma_wal_checkpoint)
	public func walCheckpoint(type: WALCheckpointType = .passive) throws {
		let mode: Int32
		switch type {
		case .passive:		mode = SQLITE_CHECKPOINT_PASSIVE
		case .full:			mode = SQLITE_CHECKPOINT_FULL
		case .restart:		mode = SQLITE_CHECKPOINT_RESTART
		case .truncate:		mode = SQLITE_CHECKPOINT_TRUNCATE
		}

		guard sqlite3_wal_checkpoint_v2(db, nil, mode, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error performing WAL checkpoint", takingDescriptionFromDatabase: db)
		}
	}
}

extension Database {
	/// Compiles and stores an SQL statement for later use.
	///
	/// - parameter sql: The SQL statement to prepare
	/// - parameter key: A key used to identify the statement
	///
	/// - throws: An error if `sql` could not be compiled
	///
	/// - returns: A compiled SQL statement
	public func prepareStatement(sql: String, forKey key: AnyHashable) throws {
		preparedStatements[key] = try prepare(sql: sql)
	}

	/// Returns the compiled SQL statement for `key`.
	///
	/// - parameter key: The key used to identify the statement
	///
	/// - returns: A compiled SQL statement or `nil` if no statement for the specified key was found
	public func preparedStatement(forKey key: AnyHashable) -> Statement? {
		return preparedStatements[key]
	}

	/// Stores a compiled SQL statement for later use.
	///
	/// - parameter statement: A compiled SQL statement
	/// - parameter key: A key used to identify the statement
	public func setPreparedStatement(_ statement: Statement, forKey key: AnyHashable) {
//		precondition(statement.database.db == self.db)
		preparedStatements[key] = statement
	}

	/// Removes a compiled SQL statement.
	///
	/// - parameter key: The key used to identify the statement
	///
	/// - returns: The statement that was removed, or `nil` if the key was not present
	public func removePreparedStatement(forKey key: AnyHashable) -> Statement? {
		return preparedStatements.removeValue(forKey: key)
	}

	/// Executes the compiled SQL statement for `key` and after execution resets the statement.
	///
	/// This method does not clear bound host parameters.
	///
	/// - parameter key: The key used to identify the statement
	/// - parameter block: A closure performing the statement operation
	/// - parameter statement: A `Statement` used for statement access within `block`
	///
	/// - throws: An error if no statement for the specified key was found, any error thrown by `block`, or an error if the statement couldn't be reset
	///
	/// - returns: The value returned by `block`
	public func withPreparedStatement<T>(forKey key: AnyHashable, _ block: (_ statement: Statement) throws -> T) throws -> T {
		guard let statement = preparedStatements[key] else {
			throw DatabaseError("No prepared statement for key \"\(key)\"")
		}
		do {
			let result = try block(statement)
			try statement.reset()
			return result
		}
		catch let error {
			try? statement.reset()
			throw error
		}
	}

	/// Returns or stores the compiled SQL statement for `key`.
	///
	/// - parameter key: The key used to identify the statement
	public subscript(key: AnyHashable) -> Statement? {
		get {
			return preparedStatements[key]
		}
		set(newValue) {
			preparedStatements[key] = newValue
		}
	}
}

extension Database {
	/// A callback for reporting the progress of a database backup.
	///
	/// - parameter remaining: The number of database pages left to copy
	/// - parameter total: The total number of database pages
	public typealias BackupProgress = (_ remaining: Int, _ total: Int) -> Void

	/// Backs up the database to the specified URL.
	///
	/// - parameter url: The destination for the backup.
	/// - parameter callback: An optional closure to receive progress information
	///
	/// - throws: An error if the backup could not be completed
	///
	/// - seealso: [Online Backup API](https://www.sqlite.org/c3ref/backup_finish.html)
	/// - seealso: [Using the SQLite Online Backup API](https://www.sqlite.org/backup.html)
	public func backup(to url: URL, progress callback: BackupProgress? = nil) throws {
		let destination = try Database(url: url)

		if let backup = sqlite3_backup_init(destination.db, "main", self.db, "main") {
			var result: Int32
			repeat {
				result = sqlite3_backup_step(backup, 5)
				callback?(Int(sqlite3_backup_remaining(backup)), Int(sqlite3_backup_pagecount(backup)))
				if result == SQLITE_OK || result == SQLITE_BUSY || result == SQLITE_LOCKED {
					sqlite3_sleep(250)
				}
			} while result == SQLITE_OK || result == SQLITE_BUSY || result == SQLITE_LOCKED

			sqlite3_backup_finish(backup)

			guard sqlite3_errcode(destination.db) == SQLITE_OK else {
				throw SQLiteError("Unable to backup database", takingDescriptionFromDatabase: destination.db)
			}
		}
	}
}

extension Database {
	/// Available database status parameters.
	///
	/// - seealso: [Status Parameters for database connections](https://www.sqlite.org/c3ref/c_dbstatus_options.html)
	public enum	StatusParameter {
		/// The number of lookaside memory slots currently checked out
		case lookasideUsed
		/// The approximate number of bytes of heap memory used by all pager caches
		case cacheUsed
		/// The approximate number of bytes of heap memory used to store the schema for all databases
		case schemaUsed
		/// The approximate number of bytes of heap and lookaside memory used by all prepared statements
		case stmtUsed
		/// The number malloc attempts that were satisfied using lookaside memory
		case lookasideHit
		/// The number malloc attempts that might have been satisfied using lookaside memory but failed due to the amount of memory requested being larger than the lookaside slot size
		case lookasideMissSize
		/// The number malloc attempts that might have been satisfied using lookaside memory but failed due to all lookaside memory already being in use
		case lookasideMissFull
		/// The number of pager cache hits that have occurred
		case cacheHit
		/// The number of pager cache misses that have occurred
		case cacheMiss
		/// The number of dirty cache entries that have been written to disk
		case cacheWrite
		/// Returns zero for the current value if and only if all foreign key constraints (deferred or immediate) have been resolved
		case deferredForeignKeys
		/// Similar to `cacheUsed` except that if a pager cache is shared between two or more connections the bytes of heap memory used by that pager cache is divided evenly between the attached connections
		case cacheUsedShared
	}

	/// Returns status information on the current and highwater values of a database parameter.
	///
	/// Not all parameters support both current and highwater values.
	///
	/// - parameter parameter: The desired database parameter
	/// - parameter resetHighwater: If `true` the highwater mark, if applicable, is reset to the current value
	///
	/// - returns: A tuple containing the current and highwater values of the requested parameter, as applicable
	///
	/// - seealso: [Database Connection Status](https://www.sqlite.org/c3ref/db_status.html)
	public func status(ofParameter parameter: StatusParameter, resetHighwater: Bool = false) throws -> (Int, Int) {
		let op: Int32
		switch parameter {
		case .lookasideUsed: 		op = SQLITE_DBSTATUS_LOOKASIDE_USED
		case .cacheUsed:			op = SQLITE_DBSTATUS_CACHE_USED
		case .schemaUsed:			op = SQLITE_DBSTATUS_SCHEMA_USED
		case .stmtUsed:				op = SQLITE_DBSTATUS_STMT_USED
		case .lookasideHit:			op = SQLITE_DBSTATUS_LOOKASIDE_HIT
		case .lookasideMissSize:	op = SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE
		case .lookasideMissFull:	op = SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL
		case .cacheHit:				op = SQLITE_DBSTATUS_CACHE_HIT
		case .cacheMiss:			op = SQLITE_DBSTATUS_CACHE_MISS
		case .cacheWrite:			op = SQLITE_DBSTATUS_CACHE_WRITE
		case .deferredForeignKeys:	op = SQLITE_DBSTATUS_DEFERRED_FKS
		case .cacheUsedShared:		op = SQLITE_DBSTATUS_CACHE_USED_SHARED
		}

		var current: Int32 = 0
		var highwater: Int32 = 0

		guard sqlite3_db_status(db, op, &current, &highwater, resetHighwater ? 1 : 0) == SQLITE_OK else {
			throw SQLiteError("Error retrieving database status", takingDescriptionFromDatabase: db)
		}

		return (Int(current), Int(highwater))
	}
}
