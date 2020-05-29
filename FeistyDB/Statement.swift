//
// Copyright (c) 2015 - 2018 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation
import os.log

/// An `sqlite3_stmt *` object.
///
/// - seealso: [SQLite Prepared Statement Object](https://sqlite.org/c3ref/stmt.html)
public typealias SQLitePreparedStatement = OpaquePointer

/// A compiled SQL statement with support for SQL parameter binding and result row processing.
///
/// **Creation**
///
/// A statement is not created directly but is obtained from a `Database`.
///
/// ```swift
/// let statement = try db.prepare(sql: "select count(*) from t1;")
/// ```
///
/// **Parameter Binding**
///
/// A statement supports binding values to SQL parameters by index or by name.
///
/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
///
/// ```swift
/// let statement = try db.prepare(sql: "insert into t1(a, b, c, d, e, f) values (?, ?, ?, :d, :e, :f);")
/// try statement.bind(value: 30, toParameter: 3)
/// try statement.bind(value: 40, toParameter: ":d")
/// try statement.bind(parameterValues: 10, 20)
/// try statement.bind(parameters: [":f": 60, ":e": 50])
/// ```
///
/// **Result Rows**
///
/// When executed a statement provides zero or more result rows.
///
/// ```swift
/// try statement.results { row in
///     // Do something with `row`
/// }
/// ```
///
/// ```swift
/// for row in statement {
///     // Do something with `row`
/// }
/// ```
///
/// It is generally preferred to use the block-based method because any errors may be explicitly handled instead of
/// silently discarded.
public final class Statement {
	/// The owning database
	public let database: Database

	/// The underlying `sqlite3_stmt *` object
	var stmt: SQLitePreparedStatement

	/// Creates a compiled SQL statement.
	///
	/// - parameter database: The owning database
	/// - parameter sql: The SQL statement to compile
	///
	/// - throws: An error if `sql` could not be compiled
	init(database: Database, sql: String) throws {
		self.database = database

		var stmt: SQLitePreparedStatement? = nil
		guard sqlite3_prepare_v2(database.db, sql, -1, &stmt, nil) == SQLITE_OK else {
			throw SQLiteError("Error preparing SQL \"\(sql)\"", takingDescriptionFromDatabase: database.db)
		}

		self.stmt = stmt!
	}

	deinit {
		sqlite3_finalize(stmt)
	}

	/// `true` if this statement makes no direct changes to the database, `false` otherwise.
	///
	/// - seealso: [Read-only statements in SQLite](https://sqlite.org/c3ref/stmt_readonly.html)
	public lazy var isReadOnly: Bool = {
		return sqlite3_stmt_readonly(self.stmt) != 0
	}()

	/// The number of SQL parameters in this statement
	public lazy var parameterCount: Int = {
		return Int(sqlite3_bind_parameter_count(self.stmt))
	}()

	/// The original SQL text of the statement
	public lazy var sql: String = {
		return String(cString: sqlite3_sql(self.stmt))
	}()

	/// The SQL text of the statement with bound parameters expanded
	public var expandedSQL: String {
		guard let s = sqlite3_expanded_sql(stmt) else {
			os_log("sqlite3_expanded_sql() returned NULL", type: .info);
			return ""
		}
		defer {
			sqlite3_free(s)
		}
		return String(cString: s)
	}

	/// The number of columns in the result set
	public lazy var columnCount: Int = {
		return Int(sqlite3_column_count(self.stmt))
	}()

	/// The mapping of column names to indexes
	lazy var columnNamesAndIndexes: [String: Int] = {
		let columnCount = sqlite3_column_count(self.stmt)
		var map = [String: Int](minimumCapacity: Int(columnCount))
		for i in 0..<columnCount {
			let name = String(cString: sqlite3_column_name(self.stmt, i))
			map[name] = Int(i)
		}
		return map
	}()

	/// Returns the name of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - throws: An error if `index` is out of bounds
	///	
	/// - returns: The name of the column for the specified index
	public func name(ofColumn index: Int) throws -> String {
		guard let name = sqlite3_column_name(stmt, Int32(index)) else {
			throw DatabaseError("Column index \(index) out of bounds")
		}
		return String(cString: name)
	}

	/// Returns the index of the column `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - throws: An error if the column doesn't exist
	///
	/// - returns: The index of the column with the specified name
	public func index(ofColumn name: String) throws -> Int {
		guard let index = columnNamesAndIndexes[name] else {
			throw DatabaseError("Unknown column \"\(name)\"")
		}
		return index
	}

	/// Executes the statement.
	///
	/// - requires: The statement does not return any result rows
	///
	/// - throws: An error if the statement returned any result rows or did not successfully run to completion
	public func execute() throws {
		switch sqlite3_step(stmt) {
		case SQLITE_DONE:
			break
		case SQLITE_ROW:
			throw DatabaseError("Result rows may not be discarded")
		default:
			throw SQLiteError("Error executing statement", takingDescriptionFromStatement: stmt)
		}
	}

	/// Executes the statement and applies `block` to each result row.
	///
	/// - parameter block: A closure applied to each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if the statement did not successfully run to completion
	public func results(_ block: ((_ row: Row) throws -> ())) throws {
		var result = sqlite3_step(stmt)
		while result == SQLITE_ROW {
			try block(Row(statement: self))
			result = sqlite3_step(stmt)
		}

		guard result == SQLITE_DONE else {
			throw SQLiteError("Error executing statement", takingDescriptionFromStatement: stmt)
		}
	}

	/// Returns the next result row or `nil` if none.
	///
	/// - returns: The next result row of returned data
	///
	/// - throws: An error if the statement encountered an execution error
	public func nextRow() throws -> Row? {
		switch sqlite3_step(stmt) {
		case SQLITE_ROW:
			return Row(statement: self)
		case SQLITE_DONE:
			return nil
		default:
			throw SQLiteError("Error executing statement", takingDescriptionFromStatement: stmt)
		}
	}

	/// Resets the statement to its initial state, ready to be re-executed.
	///
	/// - note: This function does not change the value of  any bound SQL parameters.
	///
	/// - throws: An error if the statement could not be reset
	public func reset() throws {
		guard sqlite3_reset(stmt) == SQLITE_OK else {
			throw SQLiteError("Error resetting statement", takingDescriptionFromStatement: stmt)
		}
	}

	/// Clears all statement bindings by setting SQL parameters to null.
	///
	/// - throws: An error if the bindings could not be cleared
	public func clearBindings() throws {
		guard sqlite3_clear_bindings(stmt) == SQLITE_OK else {
			throw SQLiteError("Error clearing bindings", takingDescriptionFromStatement: stmt)
		}
	}

	/// Performs a low-level SQLite statement operation.
	///
	/// **Use of this function should be avoided whenever possible**
	///
	/// - parameter block: A closure performing the operation
	/// - parameter stmt: The raw `sqlite3_stmt *` statement object
	///
	/// - throws: Any error thrown in `block`
	///
	/// - returns: The value returned by `block`
	public func withUnsafeRawSQLiteStatement<T>(block: (_ stmt: SQLitePreparedStatement) throws -> (T)) rethrows -> T {
		return try block(stmt)
	}
}

extension Statement {
	/// Returns the first result row or `nil` if none.
	///
	/// - throws: An error if the statement encountered an execution error
	///
	/// - returns: The first result row
	public func firstRow() throws -> Row? {
		return try nextRow()
	}
}

extension Statement: Sequence {
	/// Returns an iterator for accessing the result rows.
	///
	/// Because the iterator discards errors, the preferred way of accessing result rows
	/// is via `nextRow()` or `results(_:)`
	///
	/// - returns: An iterator over the result rows
	public func makeIterator() -> Statement {
		return self
	}
}

extension Statement: IteratorProtocol {
	/// Returns the next result row or `nil` if none.
	///
	/// Because the iterator discards errors, the preferred way of accessing result rows
	/// is via `nextRow()` or `results(_:)`
	///
	/// - returns: The next result row of returned data
	public func next() -> Row? {
		return try? nextRow()
	}
}

extension Statement {
	/// Available statement counters.
	///
	/// - seealso: [Status Parameters for prepared statements](http://www.sqlite.org/c3ref/c_stmtstatus_counter.html)
	public enum	Counter {
		/// The number of times that SQLite has stepped forward in a table as part of a full table scan
		case fullscanStep
		/// The number of sort operations that have occurred
		case sort
		/// The number of rows inserted into transient indices that were created automatically in order to help joins run faster
		case autoindex
		/// The number of virtual machine operations executed by the prepared statement
		case vmStep
		/// The number of times that the prepare statement has been automatically regenerated due to schema changes or change to bound parameters that might affect the query plan
		case reprepare
		/// The number of times that the prepared statement has been run
		case run
		/// The approximate number of bytes of heap memory used to store the prepared statement
		case memused
	}

	/// Returns information on a statement counter.
	///
	/// - parameter counter: The desired statement counter
	/// - parameter reset: If `true` the counter is reset to zero
	///
	/// - returns: The current value of the counter
	///
	/// - seealso: [Prepared Statement Status](http://www.sqlite.org/c3ref/stmt_status.html)
	public func count(of counter: Counter, reset: Bool = false) -> Int {
		let op: Int32
		switch counter {
		case .fullscanStep: 	op = SQLITE_STMTSTATUS_FULLSCAN_STEP
		case .sort:				op = SQLITE_STMTSTATUS_SORT
		case .autoindex:		op = SQLITE_STMTSTATUS_AUTOINDEX
		case .vmStep:			op = SQLITE_STMTSTATUS_VM_STEP
		case .reprepare:		op = SQLITE_STMTSTATUS_REPREPARE
		case .run:				op = SQLITE_STMTSTATUS_RUN
		case .memused:			op = SQLITE_STMTSTATUS_MEMUSED
		}

		return Int(sqlite3_stmt_status(stmt, op, reset ? 1 : 0))
	}
}
