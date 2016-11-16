//
// Copyright (c) 2015 - 2016 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

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
/// try statement.execute() { row in
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
final public class Statement {
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
			throw DatabaseError(message: "Error preparing SQL \"\(sql)\"", takingDescriptionFromDatabase: database.db)
		}

		self.stmt = stmt!
	}

	deinit {
		sqlite3_finalize(stmt)
	}

	/// `true` if this statement makes no direct changes to the database, `false` otherwise.
	///
	/// - seealso: [Read-only statements in SQLite](https://sqlite.org/c3ref/stmt_readonly.html)
	public lazy var readOnly: Bool = {
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
			#if DEBUG
				print("sqlite3_expanded_sql() returned NULL")
			#endif
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

	/// Executes the statement and discards any result rows.
	///
	/// - throws: An error if the statement did not successfully run to completion
	public func execute() throws {
		var result = sqlite3_step(stmt)
		while result == SQLITE_ROW {
			result = sqlite3_step(stmt)
		}

		guard result == SQLITE_DONE else {
			throw DatabaseError(message: "Error executing statement", takingDescriptionFromStatement: stmt)
		}
	}

	/// Executes the statement and applies `block` to each result row.
	///
	/// - parameter block: A closure applied to each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if the statement did not successfully run to completion
	public func execute(_ block: ((_ row: Row) throws -> ())) throws {
		var result = sqlite3_step(stmt)
		while result == SQLITE_ROW {
			try block(Row(statement: self))
			result = sqlite3_step(stmt)
		}

		guard result == SQLITE_DONE else {
			throw DatabaseError(message: "Error executing statement", takingDescriptionFromStatement: stmt)
		}
	}

	/// Resets the statement to its initial state, ready to be re-executed.
	///
	/// - note: This function does not change the value of  any bound SQL parameters.
	///
	/// - throws: An error if the statement could not be reset
	public func reset() throws {
		guard sqlite3_reset(stmt) == SQLITE_OK else {
			throw DatabaseError(message: "Error resetting statement", takingDescriptionFromStatement: stmt)
		}
	}

	/// Clears all statement bindings by setting SQL parameters to null.
	///
	/// - throws: An error if the bindings could not be cleared
	public func clearBindings() throws {
		guard sqlite3_clear_bindings(stmt) == SQLITE_OK else {
			throw DatabaseError(message: "Error clearing bindings", takingDescriptionFromStatement: stmt)
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

extension Statement: Sequence {
	/// Returns an iterator for accessing the result rows.
	///
	/// Because the iterator discards errors, the preferred way of accessing result rows
	/// is via the block-based `execute(_:)` function
	///
	/// - returns: An iterator over the result rows
	public func makeIterator() -> AnyIterator<Row> {
		return AnyIterator {
			let stmt = self.stmt
			switch sqlite3_step(stmt) {
			case SQLITE_ROW:
				return Row(statement: self)
			case SQLITE_DONE:
				return nil
			default:
				#if DEBUG
					print("Error executing statement: \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
				#endif
				return nil
			}
		}
	}
}

extension Statement {
	/// Returns the first result row.
	///
	/// - throws: An error if the statement returned no rows or encountered an execution error
	///
	/// - returns: The first result row
	public func firstRow() throws -> Row {
		let stmt = self.stmt
		switch sqlite3_step(stmt) {
		case SQLITE_ROW:
			return Row(statement: self)
		case SQLITE_DONE:
			throw DatabaseError("Statement returned no rows")
		default:
			throw DatabaseError(message: "Error executing statement", takingDescriptionFromStatement: stmt)
		}
	}
}
