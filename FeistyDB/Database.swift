/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

// C -> Swift Hacks
let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// An `sqlite3 *` object
///
/// - seealso: [SQLite Database Connection Handle](http://sqlite.org/c3ref/sqlite3.html)
public typealias SQLiteDatabaseConnection = OpaquePointer

/// A class encapsulating access to an [SQLite](http://sqlite.org) database
final public class Database {
	/// The underlying `sqlite3 *` database
	var db: SQLiteDatabaseConnection

	/// Prepared statements
	var preparedStatements = [String: Statement]()

	/// Create an in-memory database
	///
	/// - throws: `DatabaseError`
	public init() throws {
		var db: SQLiteDatabaseConnection?
		guard sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) == SQLITE_OK else {
			#if DEBUG
				print("Error creating in-memory database: \(String(cString: sqlite3_errmsg(db)))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
		}

		self.db =  db!
	}

	/// Open a database
	///
	/// - parameter url: The location of the database
	/// - parameter readOnly: Whether to open the database in read-only mode
	/// - parameter create: Whether to create the database if it doesn't exist
	/// - throws: `DatabaseError`
	public init(url: URL, readOnly: Bool = false, create: Bool = true) throws {
		var db: SQLiteDatabaseConnection?
		try url.withUnsafeFileSystemRepresentation { path in
			var flags = (readOnly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE)
			if create {
				flags |= SQLITE_OPEN_CREATE
			}

			guard sqlite3_open_v2(path, &db, flags, nil) == SQLITE_OK else {
				#if DEBUG
					print("Error opening database at \(url): \(String(cString: sqlite3_errmsg(db)))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
			}
		}

		self.db = db!
	}

	/// Initialize a database from an existing `sqlite3 *` database
	///
	/// **The database takes ownership of the passed-in database**
	///
	/// - parameter db: An `sqlite3 *` database
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

	/// Perform a low-level database operation
	///
	/// **Use of this function should be avoided whenever possible**
	///
	/// - parameter block: The block performing the operation
	/// - parameter db: The raw `sqlite3 *` database object
	/// - throws: Any error thrown in `block`
	/// - returns: The value returned by `block`
	public func withUnsafeRawSQLiteDatabase<T>(block: (_ db: SQLiteDatabaseConnection) throws -> (T)) rethrows -> T {
		return try block(self.db)
	}

	/// Perform a transaction on the database
	///
	/// - parameter type: The type of transaction to perform
	/// - parameter block: The block performing the transaction
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: If set to `true` by `block` the transaction will be rolled back
	/// - throws: Any error thrown in `block`
	/// - returns: The value returned by `block`
	public func transaction<T>(type: TransactionType = .deferred, _ block: (Database, inout Bool) throws -> (T)) throws -> T {
		var rollback = false

		guard sqlite3_exec(db, "BEGIN \(type) TRANSACTION;", nil, nil, nil) == SQLITE_OK else {
			#if DEBUG
				print("Error beginning transaction: \(String(cString: sqlite3_errmsg(db)))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
		}

		let result = try block(self, &rollback)

		if rollback {
			guard sqlite3_exec(db, "ROLLBACK TRANSACTION;", nil, nil, nil) == SQLITE_OK else {
				#if DEBUG
					print("Error rolling back transaction: \(String(cString: sqlite3_errmsg(db)))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
			}
		}
		else {
			guard sqlite3_exec(db, "COMMIT TRANSACTION;", nil, nil, nil) == SQLITE_OK else {
				#if DEBUG
					print("Error committing transaction: \(String(cString: sqlite3_errmsg(db)))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
			}
		}

		return result
	}

	/// Perform a savepoint on the database
	///
	/// - parameter block: The block performing the read/write
	/// - parameter database: The `Database` used for database access
	/// - parameter rollback: Whether to rollback the savepoint after `block` completes
	/// - returns: The value returned by `block`
	public func savepoint<T>(_ block: (Database, inout Bool) throws -> (T)) throws -> T {
		var rollback = false

		let savepointUUID = UUID().uuidString
		guard sqlite3_exec(db, "SAVEPOINT \(savepointUUID);", nil, nil, nil) == SQLITE_OK else {
			#if DEBUG
				print("Error creating savepoint: \(String(cString: sqlite3_errmsg(db)))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
		}

		let result = try block(self, &rollback)

		if rollback {
			guard sqlite3_exec(db, "ROLLBACK TO \(savepointUUID);", nil, nil, nil) == SQLITE_OK else {
				#if DEBUG
					print("Error rolling back savepoint: \(String(cString: sqlite3_errmsg(db)))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
			}
		}

		guard sqlite3_exec(db, "RELEASE SAVEPOINT \(savepointUUID);", nil, nil, nil) == SQLITE_OK else {
			#if DEBUG
				print("Error releasing savepoint: \(String(cString: sqlite3_errmsg(db)))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(db)))
		}

		return result
	}

	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - throws: `DatabaseError`
	public func execute(sql: String) throws {
		try prepare(sql: sql).execute()
	}

	/// Prepare an SQL statement
	///
	/// - parameter sql: The SQL statement to prepare
	/// - returns: A `Statement`
	/// - throws: `DatabaseError`
	public func prepare(sql: String) throws -> Statement {
		return try Statement(database: self, sql: sql)
	}

	/// Prepare and store an SQL statement for later use
	///
	/// - parameter sql: The SQL statement to prepare
	/// - parameter key: A key used to identify the statement
	/// - throws: `DatabaseError`
	public func prepareStatement(sql: String, forKey key: String) throws {
		preparedStatements[key] = try prepare(sql: sql)
	}

	/// Retrieve a prepared statement
	///
	/// - parameter key: The key used to identify the statement
	/// - returns: A prepared SQL statement or `nil` if no statement for the specified key was found
	public func preparedStatement(forKey key: String) -> Statement? {
		return preparedStatements[key]
	}

	/// Remove a prepared statement
	///
	/// - parameter key: The key used to identify the statement
	/// - returns: The statement that was removed, or `nil` if the key was not present
	public func removePreparedStatement(forKey key: String) -> Statement? {
		return preparedStatements.removeValue(forKey: key)
	}
}
