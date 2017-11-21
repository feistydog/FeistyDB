//
// Copyright (c) 2015 - 2017 Feisty Dog, LLC
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

	/// Prepared statements
	var preparedStatements = [String: Statement]()

	/// Creates an in-memory database.
	///
	/// - throws: An error if the database could not be created
	public init() throws {
		var db: SQLiteDatabaseConnection?
		guard sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) == SQLITE_OK else {
			throw DatabaseError(message: "Error creating in-memory database", takingDescriptionFromDatabase: db!)
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
				throw DatabaseError(message: "Error opening database at \(url)", takingDescriptionFromDatabase: db!)
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
	}

	/// `true` if this database is read only, `false` otherwise
	public lazy var readOnly: Bool = {
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

	/// Returns the maximum amount of memory used by SQLite since the memory high-water mark was last reset.
	///
	/// - parameter reset: If `true` the memory high-water mark is reset to the value of `memoryUsed`
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
			throw DatabaseError(message: "Error beginning transaction", takingDescriptionFromDatabase: db)
		}
	}

	/// Rolls back the active database transaction.
	///
	/// - throws: An error if the transaction couldn't be rolled back or there is no active transaction
	public func rollback() throws {
		guard sqlite3_exec(db, "ROLLBACK;", nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError(message: "Error rolling back", takingDescriptionFromDatabase: db)
		}
	}

	/// Commits the active database transaction.
	///
	/// - throws: An error if the transaction couldn't be committed or there is no active transaction
	public func commit() throws {
		guard sqlite3_exec(db, "COMMIT;", nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError(message: "Error committing", takingDescriptionFromDatabase: db)
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
		guard sqlite3_exec(db, "SAVEPOINT \(name);", nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError(message: "Error creating savepoint", takingDescriptionFromDatabase: db)
		}
	}

	/// Rolls back a database savepoint transaction.
	///
	/// - parameter name: The name of the savepoint transaction
	///
	/// - throws: An error if the savepoint transaction couldn't be rolled back or doesn't exist
	public func rollback(to name: String) throws {
		guard sqlite3_exec(db, "ROLLBACK TO \(name);", nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError(message: "Error rolling back savepoint", takingDescriptionFromDatabase: db)
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
			throw DatabaseError(message: "Error releasing savepoint", takingDescriptionFromDatabase: db)
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
	public func prepareStatement(sql: String, forKey key: String) throws -> Statement {
		let statement = try prepare(sql: sql)
		preparedStatements[key] = statement
		return statement
	}

	/// Returns the compiled SQL statement for `key`.
	///
	/// - parameter key: The key used to identify the statement
	///
	/// - returns: A compiled SQL statement or `nil` if no statement for the specified key was found
	public func preparedStatement(forKey key: String) -> Statement? {
		return preparedStatements[key]
	}

	/// Removes a compiled SQL statement.
	///
	/// - parameter key: The key used to identify the statement
	///
	/// - returns: The statement that was removed, or `nil` if the key was not present
	public func removePreparedStatement(forKey key: String) -> Statement? {
		return preparedStatements.removeValue(forKey: key)
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
				throw DatabaseError(message: "Unable to backup database", takingDescriptionFromDatabase: destination.db)
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
	/// try db.add(collation: "localizedCompare", { (lhs, rhs) -> ComparisonResult in
	///     return lhs.localizedCompare(rhs)
	/// })
	/// ```
	///
	/// - parameter name: The name of the custom collation sequence
	/// - parameter block: A string comparison function
	///
	/// - throws: An error if the collation function couldn't be added
	public func add(collation name: String, _ block: @escaping StringComparator) throws {
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
			function_ptr.deinitialize()
			function_ptr.deallocate(capacity: 1)
		}) == SQLITE_OK else {
			throw DatabaseError(message: "Error adding collation sequence \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}

	/// Removes a custom collation function.
	///
	/// - parameter name: The name of the custom collation sequence
	///
	/// - throws: An error if the collation function couldn't be removed
	public func remove(collation name: String) throws {
		guard sqlite3_create_collation_v2(db, name, SQLITE_UTF8, nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError(message: "Error removing collation sequence \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}
}
extension Database {
	/// A custom SQL function.
	///
	/// - parameter values: The SQL function parameters
	///
	/// - throws: `DatabaseError`
	///
	/// - returns: The result of applying the function to `values`
	public typealias SQLFunction = (_ values: [DatabaseValue]) throws -> DatabaseValue

	/// Adds a custom SQL function.
	///
	/// ```swift
	/// try db.add(function: "localizedUppercase", arity: 1) { values in
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
	public func add(function name: String, arity: Int = -1, _ block: @escaping SQLFunction) throws {
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
			function_ptr.deinitialize()
			function_ptr.deallocate(capacity: 1)
		}) == SQLITE_OK else {
			throw DatabaseError(message: "Error adding SQL function \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}

	/// Removes a custom SQL function.
	///
	/// - parameter name: The name of the custom SQL function
	/// - parameter arity: The number of arguments the custom SQL functions accepts
	///
	/// - throws: An error if the SQL function couldn't be removed
	public func remove(function name: String, arity: Int = -1) throws {
		guard sqlite3_create_function_v2(db, name, Int32(arity), SQLITE_UTF8 | SQLITE_DETERMINISTIC, nil, nil, nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError(message: "Error removing SQL function \"\(name)\"", takingDescriptionFromDatabase: db)
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
	public func set(commitHook block: @escaping CommitHook) {
		let context = UnsafeMutablePointer<CommitHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_commit_hook(db, { context in
			return context.unsafelyUnwrapped.assumingMemoryBound(to: CommitHook.self).pointee() ? 0 : 1
		}, context) {
			let oldContext = old.assumingMemoryBound(to: CommitHook.self)
			oldContext.deinitialize()
			oldContext.deallocate(capacity: 1)
		}
	}

	/// Removes the commit hook.
	public func removeCommitHook() {
		if let old = sqlite3_commit_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: CommitHook.self)
			oldContext.deinitialize()
			oldContext.deallocate(capacity: 1)
		}
	}

	/// A hook called when a database transaction is rolled back.
	///
	/// - seealso: [Commit And Rollback Notification Callbacks](http://www.sqlite.org/c3ref/commit_hook.html)
	public typealias RollbackHook = () -> Void

	/// Sets the hook called when a database transaction is rolled back.
	///
	/// - parameter rollbackHook: A closure called when a transaction is rolled back
	public func set(rollbackHook block: @escaping RollbackHook) {
		let context = UnsafeMutablePointer<RollbackHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_rollback_hook(db, { context in
			context.unsafelyUnwrapped.assumingMemoryBound(to: RollbackHook.self).pointee()
		}, context) {
			let oldContext = old.assumingMemoryBound(to: RollbackHook.self)
			oldContext.deinitialize()
			oldContext.deallocate(capacity: 1)
		}
	}

	/// Removes the rollback hook.
	public func removeRollbackHook() {
		if let old = sqlite3_rollback_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: RollbackHook.self)
			oldContext.deinitialize()
			oldContext.deallocate(capacity: 1)
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
	public func set(walCommitHook block: @escaping walCommitHook) {
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
			oldContext.deinitialize()
			oldContext.deallocate(capacity: 1)
		}
	}

	/// Removes the write-ahead log commit hook.
	public func removeWALCommitHook() {
		if let old = sqlite3_wal_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: walCommitHook.self)
			oldContext.deinitialize()
			oldContext.deallocate(capacity: 1)
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
	public func set(updateHook block: @escaping UpdateHook) {
		let context = UnsafeMutablePointer<UpdateHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_update_hook(db, { context, op, db_name, table_name, rowid in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: UpdateHook.self)

			let changeType: RowChangeType
			switch op {
				case SQLITE_INSERT: 	changeType = .insert
				case SQLITE_DELETE: 	changeType = .delete
				case SQLITE_UPDATE: 	changeType = .update
				default:				fatalError("Unexpected row change type")
			}

			let database = String(utf8String: db_name.unsafelyUnwrapped).unsafelyUnwrapped
			let table = String(utf8String: table_name.unsafelyUnwrapped).unsafelyUnwrapped

			function_ptr.pointee(changeType, database, table, rowid)
		}, context) {
			let oldContext = old.assumingMemoryBound(to: UpdateHook.self)
			oldContext.deinitialize()
			oldContext.deallocate(capacity: 1)
		}
	}

	/// Removes the update hook.
	public func removeUpdateHook() {
		if let old = sqlite3_update_hook(db, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: UpdateHook.self)
			oldContext.deinitialize()
			oldContext.deallocate(capacity: 1)
		}
	}
}
