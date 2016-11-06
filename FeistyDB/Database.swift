/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

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
/// let rowCount: Int
/// let db = try Database()
/// try db.execute(sql: "select count(*) from t1;") { row in
///     rowCount = try row.value(at: 0)
/// }
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
			throw DatabaseError.sqliteError("Error creating in-memory database: \(String(cString: sqlite3_errmsg(db)))")
		}

		self.db =  db!
	}

	/// Creates a database from a file.
	///
	/// - parameter url: The location of the database
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
				throw DatabaseError.sqliteError("Error opening database at \(url): \(String(cString: sqlite3_errmsg(db)))")
			}
		}

		self.db = db!
	}

	/// Creates a database from an existing `sqlite3 *` database connection handle.
	///
	/// **The database takes ownership of the passed-in database connection handle**
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

	/// Performs a low-level SQLite database operation.
	///
	/// **Use of this function should be avoided whenever possible**
	///
	/// - parameter block: The closure performing the database operation
	/// - parameter db: The raw `sqlite3 *` database connection handle
	///
	/// - throws: Any error thrown by `block`
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

	/// Executes an SQL statement and applies `block` to each result row.
	///
	/// This is a shortcut to `try prepare(sql: sql).execute(block)`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter block: A closure applied to each result row
	///
	/// - throws: An error if `sql` could not be compiled or executed
	public func execute(sql: String, _ block: ((_ row: Row) throws -> ())? = nil) throws {
		try prepare(sql: sql).execute(block)
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
			throw DatabaseError.sqliteError("Error beginning transaction: \(String(cString: sqlite3_errmsg(db)))")
		}
	}

	/// Rolls back the active database transaction.
	///
	/// - throws: An error if the transaction couldn't be rolled back or there is no active transaction
	public func rollback() throws {
		guard sqlite3_exec(db, "ROLLBACK;", nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error rolling back: \(String(cString: sqlite3_errmsg(db)))")
		}
	}

	/// Commits the active database transaction.
	///
	/// - throws: An error if the transaction couldn't be committed or there is no active transaction
	public func commit() throws {
		guard sqlite3_exec(db, "COMMIT;", nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error committing: \(String(cString: sqlite3_errmsg(db)))")
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
			throw DatabaseError.sqliteError("Error creating savepoint: \(String(cString: sqlite3_errmsg(db)))")
		}
	}

	/// Rolls back a database savepoint transaction.
	///
	/// - parameter name: The name of the savepoint transaction
	///
	/// - throws: An error if the savepoint transaction couldn't be rolled back or doesn't exist
	public func rollback(to name: String) throws {
		guard sqlite3_exec(db, "ROLLBACK TO \(name);", nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error rolling back savepoint: \(String(cString: sqlite3_errmsg(db)))")
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
			throw DatabaseError.sqliteError("Error releasing savepoint: \(String(cString: sqlite3_errmsg(db)))")
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

	/// Removes a compiled SQL statement.
	///
	/// - parameter key: The key used to identify the statement
	///
	/// - returns: The statement that was removed, or `nil` if the key was not present
	public func removePreparedStatement(forKey key: String) -> Statement? {
		return preparedStatements.removeValue(forKey: key)
	}
}

/// A comparator for `String` objects
///
/// - parameter lhs: The left-hand operand
/// - parameter rhs: The right-hand operand
///
/// - returns: The result of comparing `lhs` to `rhs`
public typealias StringComparator = (_ lhs: String, _ rhs: String) -> ComparisonResult

extension Database {
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
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
		}
	}

	/// Removes a custom collation function.
	///
	/// - parameter name: The name of the custom collation sequence
	///
	/// - throws: An error if the collation function couldn't be removed
	public func remove(collation name: String) throws {
		guard sqlite3_create_collation_v2(db, name, SQLITE_UTF8, nil, nil, nil) == SQLITE_OK else {
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
		}
	}
}

/// A custom SQL function.
///
/// - parameter values: The SQL function parameters
///
/// - throws: `DatabaseError`
///
/// - returns: The result of applying the function to `values`
public typealias SQLFunction = (_ values: [DatabaseValue]) throws -> DatabaseValue

// Ideally I'd like the typealias to have the following signature:
//     public typealias SQLFunction = (DatabaseValue...) throws -> DatabaseValue
// However, it isn't possible to convert an array into a variable number of arguments (yet)
// https://bugs.swift.org/browse/SR-128

extension Database {
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
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
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
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
		}
	}
}
