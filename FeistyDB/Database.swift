//
// Copyright (c) 2015 - 2018 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

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
final public class Database {
	/// The underlying `sqlite3 *` database
	var db: SQLiteDatabaseConnection

	/// The database's custom busy handler
	var busyHandler: UnsafeMutablePointer<BusyHandler>?

	/// Prepared statements
	var preparedStatements = [String: Statement]()

	/// Creates an in-memory database.
	///
	/// - throws: An error if the database could not be created
	public init() throws {
		var db: SQLiteDatabaseConnection?
		guard sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) == SQLITE_OK else {
			throw SQLiteError("Error creating in-memory database", takingDescriptionFromDatabase: db!)
		}

		self.db =  db!
	}

	/// Creates a database from a file.
	///
	/// - parameter url: The location of the SQLite database
	/// - parameter readOnly: Whether to open the database in read-only mode
	/// - parameter create: Whether to create the database if it doesn't exist
	///
	/// - throws: An error if the database could not be opened
	public init(url: URL, readOnly: Bool = false, create: Bool = true) throws {
		var db: SQLiteDatabaseConnection?
		try url.withUnsafeFileSystemRepresentation { path in
			var flags = (readOnly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE)
			if create {
				flags |= SQLITE_OPEN_CREATE
			}

			guard sqlite3_open_v2(path, &db, flags, nil) == SQLITE_OK else {
				throw SQLiteError("Error opening database at \(url)", takingDescriptionFromDatabase: db!)
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

	/// The number of bytes of memory `malloc`ed but not yet `free`d by SQLite
	public class var memoryUsed: Int64 {
		return sqlite3_memory_used()
	}

	/// Returns the maximum amount of memory used by SQLite since the memory highwater mark was last reset.
	///
	/// - parameter reset: If `true` the memory highwater mark is reset to the value of `memoryUsed`
	public class func memoryHighwater(reset: Bool = false) -> Int64 {
		return sqlite3_memory_highwater(reset ? 1 : 0)
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
	/// - seealso: [Test For Auto-Commit Mode](http://www.sqlite.org/c3ref/get_autocommit.html)
	public var isInAutocommitMode: Bool {
		return sqlite3_get_autocommit(db) != 0
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
		guard sqlite3_exec(db, "SAVEPOINT \(name);", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error creating savepoint", takingDescriptionFromDatabase: db)
		}
	}

	/// Rolls back a database savepoint transaction.
	///
	/// - parameter name: The name of the savepoint transaction
	///
	/// - throws: An error if the savepoint transaction couldn't be rolled back or doesn't exist
	public func rollback(to name: String) throws {
		guard sqlite3_exec(db, "ROLLBACK TO \(name);", nil, nil, nil) == SQLITE_OK else {
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
		guard sqlite3_exec(db, "RELEASE \(name);", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error releasing savepoint", takingDescriptionFromDatabase: db)
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
	public func prepareStatement(sql: String, forKey key: String) throws {
		preparedStatements[key] = try prepare(sql: sql)
	}

	/// Returns the compiled SQL statement for `key`.
	///
	/// - parameter key: The key used to identify the statement
	///
	/// - returns: A compiled SQL statement or `nil` if no statement for the specified key was found
	public func preparedStatement(forKey key: String) -> Statement? {
		return preparedStatements[key]
	}

	/// Stores a compiled SQL statement for later use.
	///
	/// - parameter statement: A compiled SQL statement
	/// - parameter key: A key used to identify the statement
	public func setPreparedStatement(_ statement: Statement, forKey key: String) {
//		precondition(statement.database.db == self.db)
		preparedStatements[key] = statement
	}

	/// Removes a compiled SQL statement.
	///
	/// - parameter key: The key used to identify the statement
	///
	/// - returns: The statement that was removed, or `nil` if the key was not present
	public func removePreparedStatement(forKey key: String) -> Statement? {
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
	public func withPreparedStatement<T>(forKey key: String, _ block: (_ statement: Statement) throws -> T) throws -> T {
		guard let statement = preparedStatements[key] else {
			throw DatabaseError("No prepared statement for key \"\(key)\"")
		}
		let result = try block(statement)
		try statement.reset()
		return result
	}

	/// Returns or stores the compiled SQL statement for `key`.
	///
	/// - parameter key: The key used to identify the statement
	public subscript(key: String) -> Statement? {
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
	/// - seealso: [Online Backup API](http://www.sqlite.org/c3ref/backup_finish.html)
	/// - seealso: [Using the SQLite Online Backup API](http://www.sqlite.org/backup.html)
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
	/// A comparator for `String` objects.
	///
	/// - parameter lhs: The left-hand operand
	/// - parameter rhs: The right-hand operand
	///
	/// - returns: The result of comparing `lhs` to `rhs`
	public typealias StringComparator = (_ lhs: String, _ rhs: String) -> ComparisonResult

	/// Adds a custom collation function.
	///
	/// ```swift
	/// try db.addCollation("localizedCompare", { (lhs, rhs) -> ComparisonResult in
	///     return lhs.localizedCompare(rhs)
	/// })
	/// ```
	///
	/// - parameter name: The name of the custom collation sequence
	/// - parameter block: A string comparison function
	///
	/// - throws: An error if the collation function couldn't be added
	public func addCollation(_ name: String, _ block: @escaping StringComparator) throws {
		let function_ptr = UnsafeMutablePointer<StringComparator>.allocate(capacity: 1)
		function_ptr.initialize(to: block)
		guard sqlite3_create_collation_v2(db, name, SQLITE_UTF8, function_ptr, { (context, lhs_len, lhs_data, rhs_len, rhs_data) -> Int32 in
			// Have total faith that SQLite will pass valid parameters and use unsafelyUnwrapped
			let lhs = String(bytesNoCopy: UnsafeMutableRawPointer(mutating: lhs_data.unsafelyUnwrapped), length: Int(lhs_len), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
			let rhs = String(bytesNoCopy: UnsafeMutableRawPointer(mutating: rhs_data.unsafelyUnwrapped), length: Int(rhs_len), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped

			// Cast context to the appropriate type and call the comparator
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: StringComparator.self)
			let result = function_ptr.pointee(lhs, rhs)
			return Int32(result.rawValue)
		}, { context in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: StringComparator.self)
			function_ptr.deinitialize(count: 1)
			function_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding collation sequence \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}

	/// Removes a custom collation function.
	///
	/// - parameter name: The name of the custom collation sequence
	///
	/// - throws: An error if the collation function couldn't be removed
	public func removeCollation(_ name: String) throws {
		guard sqlite3_create_collation_v2(db, name, SQLITE_UTF8, nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error removing collation sequence \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}
}

extension Database {
	/// A custom SQL function.
	///
	/// - parameter values: The SQL function parameters
	///
	/// - throws: `Error`
	///
	/// - returns: The result of applying the function to `values`
	public typealias SQLFunction = (_ values: [DatabaseValue]) throws -> DatabaseValue

	/// Adds a custom SQL function.
	///
	/// ```swift
	/// try db.addFunction("localizedUppercase", arity: 1) { values in
	///     let value = values.first.unsafelyUnwrapped
	///     switch value {
	///     case .text(let s):
	///         return .text(s.localizedUppercase())
	///     default:
	///         return value
	///     }
	/// }
	/// ```
	///
	/// - parameter name: The name of the function
	/// - parameter arity: The number of arguments the function accepts
	/// - parameter block: A closure that returns the result of applying the function to the supplied arguments
	///
	/// - throws: An error if the SQL function couldn't be added
	///
	/// - seealso: [Create Or Redefine SQL Functions](https://sqlite.org/c3ref/create_function.html)
	public func addFunction(_ name: String, arity: Int = -1, _ block: @escaping SQLFunction) throws {
		let function_ptr = UnsafeMutablePointer<SQLFunction>.allocate(capacity: 1)
		function_ptr.initialize(to: block)
		guard sqlite3_create_function_v2(db, name, Int32(arity), SQLITE_UTF8 | SQLITE_DETERMINISTIC, function_ptr, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLFunction.self)

			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { DatabaseValue($0.unsafelyUnwrapped) }

			do {
				// Call the function and pass the result to sqlite
				switch try function_ptr.pointee(arguments) {
				case .integer(let i):
					sqlite3_result_int64(sqlite_context, i)
				case .float(let f):
					sqlite3_result_double(sqlite_context, f)
				case .text(let t):
					sqlite3_result_text(sqlite_context, t, -1, SQLITE_TRANSIENT)
				case .blob(let b):
					b.withUnsafeBytes { bytes in
						sqlite3_result_blob(sqlite_context, bytes, Int32(b.count), SQLITE_TRANSIENT)
					}
				case .null:
					sqlite3_result_null(sqlite_context)
				}
			}

			catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, nil, nil, { context in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLFunction.self)
			function_ptr.deinitialize(count: 1)
			function_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding SQL function \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}

	/// Removes a custom SQL function.
	///
	/// - parameter name: The name of the custom SQL function
	/// - parameter arity: The number of arguments the custom SQL functions accepts
	///
	/// - throws: An error if the SQL function couldn't be removed
	public func removeFunction(_ name: String, arity: Int = -1) throws {
		guard sqlite3_create_function_v2(db, name, Int32(arity), SQLITE_UTF8 | SQLITE_DETERMINISTIC, nil, nil, nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error removing SQL function \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}
}

extension Database {
	/// A hook called when a database transaction is committed.
	///
	/// - returns: `true` if the commit operation is allowed to proceed, `false` otherwise
	///
	/// - seealso: [Commit And Rollback Notification Callbacks](http://www.sqlite.org/c3ref/commit_hook.html)
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
	/// - seealso: [Commit And Rollback Notification Callbacks](http://www.sqlite.org/c3ref/commit_hook.html)
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
	/// - seealso: [Write-Ahead Log Commit Hook](http://www.sqlite.org/c3ref/wal_hook.html)
	public typealias walCommitHook = (_ databaseName: String, _ pageCount: Int) -> Int32

	/// Sets the hook called when a database transaction is committed in write-ahead log mode.
	///
	/// - parameter commitHook: A closure called when a transaction is committed
	public func setWALCommitHook(_ block: @escaping walCommitHook) {
		let context = UnsafeMutablePointer<walCommitHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_wal_hook(db, { context, db, db_name, pageCount in
//			guard db == self.db else {
//				fatalError("Unexpected database connection handle from sqlite3_wal_hook")
//			}
			let database = String(utf8String: db_name.unsafelyUnwrapped).unsafelyUnwrapped
			return context.unsafelyUnwrapped.assumingMemoryBound(to: walCommitHook.self).pointee(database, Int(pageCount))
		}, context) {
			let oldContext = old.assumingMemoryBound(to: walCommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the write-ahead log commit hook.
	public func removeWALCommitHook() {
		if let old = sqlite3_wal_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: walCommitHook.self)
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
	/// - seealso: [Commit And Rollback Notification Callbacks](http://www.sqlite.org/c3ref/commit_hook.html)
	/// - seealso: [Rowid Tables](http://www.sqlite.org/rowidtable.html)
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
	/// - seealso: [Register A Callback To Handle SQLITE_BUSY Errors](http://www.sqlite.org/c3ref/busy_handler.html)
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
	/// - seealso: [Set A Busy Timeout](http://www.sqlite.org/c3ref/busy_timeout.html)
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

extension Database {
	/// Available database status parameters.
	///
	/// - seealso: [Status Parameters for database connections](http://www.sqlite.org/c3ref/c_dbstatus_options.html)
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
	/// - seealso: [Database Connection Status](http://www.sqlite.org/c3ref/db_status.html)
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

/// Returns a pointer to the `fts5_api` structure for `db`.
///
/// - parameter db: The database connection to query
///
/// - throws:  An error if the `fts5_api` structure couldn't be retrieved
///
/// - returns: A pointer to the global `fts5_api` structure for `db`
func get_fts5_api(for db: SQLiteDatabaseConnection) throws -> UnsafePointer<fts5_api> {
	var stmt: SQLitePreparedStatement? = nil
	let sql = "SELECT fts5(?1);"
	guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
		throw SQLiteError("Error preparing SQL \"\(sql)\"", takingDescriptionFromDatabase: db)
	}

	defer {
		sqlite3_finalize(stmt)
	}

	var api_ptr: UnsafePointer<fts5_api>?
	guard sqlite3_bind_pointer(stmt, 1, &api_ptr, "fts5_api_ptr", nil) == SQLITE_OK else {
		throw SQLiteError("Error binding FTS5 API pointer", takingDescriptionFromStatement: stmt!)
	}

	guard sqlite3_step(stmt) == SQLITE_ROW else {
		throw SQLiteError("Error retrieving FTS5 API pointer", takingDescriptionFromStatement: stmt!)
	}

	guard let api = api_ptr else {
		throw DatabaseError("FTS5 not available")
	}

	return api
}

// Protocol declarations can't be nested, otherwise this would be inside Database

/// An interface to a custom FTS5 tokenizer.
public protocol FTS5Tokenizer {
	/// Initializes an FTS5 tokenizer.
	///
	/// - parameter arguments: The tokenizer arguments used to create the FTS5 table.
	init(arguments: [String])

	/// Sets the text to be tokenized.
	///
	/// - parameter text: The text to be tokenized.
	/// - parameter reason: The reason tokenization is being requested.
	func setText(_ text: String, reason: Database.FTS5TokenizationReason)

	/// Advances the tokenizer to the next token.
	///
	/// - returns: `true` if a token was found, `false` otherwise
	func advance() -> Bool

	/// Returns the current token.
	///
	/// - returns: The current token or `nil` if none
	func currentToken() -> String?

	/// Copies the current token in UTF-8 to the supplied buffer.
	///
	/// - parameter buffer: A buffer to receive the current token encoded in UTF-8
	/// - parameter capacity: The number of bytes availabe in `buffer`
	///
	/// - throws: An error if `buffer` has insufficient capacity for the token
	///
	/// - returns: The number of bytes written to `buffer`
	func copyCurrentToken(to buffer: UnsafeMutablePointer<UInt8>, capacity: Int) throws -> Int
}

extension Database {
	/// Glue for creating a generic Swift type in a C callback
	class FTS5TokenizerCreator {
		/// Creates a new FTS5TokenizerCreator.
		///
		/// - parameter construct: A closure that creates the tokenizer
		init(_ construct: @escaping (_ arguments: [String]) -> FTS5Tokenizer)
		{
			self.construct = construct
		}

		/// The constructor closure
		let construct: (_ arguments : [String]) throws -> FTS5Tokenizer
	}

	/// The reasons FTS5 will request tokenization
	public enum FTS5TokenizationReason {
		/// A document is being inserted into or removed from the FTS table
		case document
		/// A `MATCH` query is being executed against the FTS index
		case query
		/// Same as `query`, except that the bareword or quoted string is followed by a `*` character
		case prefix
		/// The tokenizer is being invoked to satisfy an `fts5_api.xTokenize()` request made by an auxiliary function
		case aux
	}

	/// Adds a custom FTS5 tokenizer.
	///
	/// For example, a word tokenizer using CFStringTokenizer could be implemented as:
	/// ```swift
	/// class WordTokenizer: FTS5Tokenizer {
	/// 	var tokenizer: CFStringTokenizer!
	/// 	var text: CFString!
	///
	/// 	required init(arguments: [String]) {
	/// 		// Arguments not used
	/// 	}
	///
	/// 	func set(text: String, reason: Database.FTS5TokenizationReason) {
	/// 		// Reason not used
	/// 		self.text = text as CFString
	/// 		tokenizer = CFStringTokenizerCreate(kCFAllocatorDefault, self.text, CFRangeMake(0, CFStringGetLength(self.text)), kCFStringTokenizerUnitWord, nil)
	/// 	}
	///
	/// 	func advance() -> Bool {
	/// 		let nextToken = CFStringTokenizerAdvanceToNextToken(tokenizer)
	/// 		guard nextToken != CFStringTokenizerTokenType(rawValue: 0) else {
	/// 			return false
	/// 		}
	/// 		return true
	/// 	}
	///
	/// 	func currentToken() -> String? {
	/// 		let tokenRange = CFStringTokenizerGetCurrentTokenRange(tokenizer)
	/// 		guard tokenRange.location != kCFNotFound /*|| tokenRange.length != 0*/ else {
	/// 			return nil
	/// 		}
	/// 		return CFStringCreateWithSubstring(kCFAllocatorDefault, text, tokenRange) as String
	/// 	}
	///
	/// 	func copyCurrentToken(to buffer: UnsafeMutablePointer<UInt8>, capacity: Int) throws -> Int {
	/// 		let tokenRange = CFStringTokenizerGetCurrentTokenRange(tokenizer)
	/// 		var bytesConverted = 0
	/// 		let charsConverted = CFStringGetBytes(text, tokenRange, CFStringBuiltInEncodings.UTF8.rawValue, 0, false, buffer, capacity, &bytesConverted)
	/// 		guard charsConverted > 0 else {
	/// 			throw DatabaseError("Insufficient buffer size")
	/// 		}
	/// 		return bytesConverted
	/// 	}
	/// }
	/// ```
	///
	/// - parameter name: The name of the tokenizer
	/// - parameter type: The class implementing the tokenizer
	///
	/// - throws:  An error if the tokenizer can't be added
	///
	/// - seealso: [Custom Tokenizers](http://www.sqlite.org/fts5.html#custom_tokenizers)
	public func addTokenizer<T: FTS5Tokenizer>(_ name: String, type: T.Type) throws {
		// Fail early if FTS5 isn't available
		let api_ptr = try get_fts5_api(for: db)

		// Flesh out the struct containing the xCreate, xDelete, and xTokenize functions used by SQLite
		var tokenizer_struct = fts5_tokenizer(xCreate: { (user_data, argv, argc, out) -> Int32 in
			// Create the tokenizer instance using the creation function passed to fts5_api.xCreateTokenizer()
			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { String(utf8String: $0.unsafelyUnwrapped).unsafelyUnwrapped }

			let tokenizer: FTS5Tokenizer
			do {
				let creator = Unmanaged<FTS5TokenizerCreator>.fromOpaque(UnsafeRawPointer(user_data.unsafelyUnwrapped)).takeUnretainedValue()
				tokenizer = try creator.construct(arguments)
			}

			catch let error {
				return SQLITE_ERROR
			}

			// tokenizer must live until the xDelete function is invoked; store it as a +1 object in ptr
			let ptr = Unmanaged.passRetained(tokenizer as AnyObject).toOpaque()
			out?.initialize(to: OpaquePointer(ptr))

			return SQLITE_OK
		}, xDelete: { p in
			// Balance the +1 retain above
			Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(p.unsafelyUnwrapped)).release()
		}, xTokenize: { (tokenizer_ptr, context, flags, text_utf8, text_len, xToken) -> Int32 in
			// Tokenize the text and invoke xToken for each token found
			let tokenizer = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(tokenizer_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! FTS5Tokenizer

			// Set the text to be tokenized
			let text = String(bytesNoCopy: UnsafeMutableRawPointer(mutating: text_utf8.unsafelyUnwrapped), length: Int(text_len), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
			let reason = FTS5TokenizationReason(flags)

			tokenizer.setText(text, reason: reason)

			// Use a local buffer for token extraction if possible
			let bufsize = 512
			var buf = UnsafeMutablePointer<UInt8>.allocate(capacity: bufsize)

			defer {
				buf.deallocate()
			}

			// Process each token and pass to FTS5
			while tokenizer.advance() {
				do {
					// Attempt to copy the current token to buf
					let byteCount = try tokenizer.copyCurrentToken(to: buf, capacity: bufsize)

					let result = UnsafePointer(buf).withMemoryRebound(to: Int8.self, capacity: bufsize) { bytes in
						return xToken.unsafelyUnwrapped(context, 0, bytes, Int32(byteCount), 0, Int32(byteCount))
					}

					guard result == SQLITE_OK else {
						return result
					}
				}

				catch {
					// The token was too large to fit in buf
					guard let token = tokenizer.currentToken() else {
						continue
					}
					let utf8 = token.utf8
					let result = xToken.unsafelyUnwrapped(context, 0, token, Int32(utf8.count), Int32(utf8.startIndex.encodedOffset), Int32(utf8.endIndex.encodedOffset))
					guard result == SQLITE_OK else {
						return result
					}
				}
			}

			return SQLITE_OK
		})

		// user_data must live until the xDestroy function is invoked; store it as a +1 object
		let user_data = FTS5TokenizerCreator { (args) -> FTS5Tokenizer in
			return T(arguments: args)
		}
		let user_data_ptr = Unmanaged.passRetained(user_data).toOpaque()

		guard api_ptr.pointee.xCreateTokenizer(UnsafeMutablePointer(mutating: api_ptr), name, user_data_ptr, &tokenizer_struct, { user_data in
			// Balance the +1 retain above
			Unmanaged<FTS5TokenizerCreator>.fromOpaque(UnsafeRawPointer(user_data.unsafelyUnwrapped)).release()
		}) == SQLITE_OK else {
			// xDestroy is not called if fts5_api.xCreateTokenizer() fails
			Unmanaged<FTS5TokenizerCreator>.fromOpaque(user_data_ptr).release()
			throw SQLiteError("Error creating FTS5 tokenizer", takingDescriptionFromDatabase: db)
		}
	}
}

extension Database.FTS5TokenizationReason {
	/// Convenience initializer for conversion of `FTS5_TOKENIZE_` values
	///
	/// - parameter flags: The flags passed as the second argument of `fts5_tokenizer.xTokenize()`
	init(_ flags: Int32) {
		switch flags {
		case FTS5_TOKENIZE_DOCUMENT: 						self = .document
		case FTS5_TOKENIZE_QUERY: 							self = .query
		case FTS5_TOKENIZE_QUERY | FTS5_TOKENIZE_PREFIX: 	self = .prefix
		case FTS5_TOKENIZE_AUX: 							self = .aux
		default:											preconditionFailure("Unexpected FTS5 flag")
		}
	}
}
