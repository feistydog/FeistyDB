//
// Copyright (c) 2015 - 2017 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// A type that can be initialized directly from a column in an SQLite result row.
///
/// The implementation should use one of the `sqlite_column_X()` functions documented at [Result Values From A Query](https://sqlite.org/c3ref/column_blob.html).
///
/// For example, the implementation for `Int64` is:
///
/// ```swift
/// extension Int64: ColumnConvertible {
///     public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
///        self = sqlite3_column_int64(stmt, idx)
///     }
/// }
///  ```
public protocol ColumnConvertible {
	/// Creates an instance containing the value of column `idx` in `stmt`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - precondition: `sqlite3_column_type(stmt, idx) != SQLITE_NULL`
	///
	/// - parameter stmt: An `sqlite3_stmt *` object
	/// - parameter idx: The index of the desired column
	///
	/// - throws: An error if initialization fails
	init(_ stmt: SQLitePreparedStatement, column idx: Int32) throws
}

extension Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - throws: An error if `index` is out of bounds or the column contains an illegal value
	///
	/// - returns: The column's value or `nil` if null
	public func value<T: ColumnConvertible>(at index: Int) throws -> T? {
		guard index >= 0, index < self.columnCount else {
			throw DatabaseError("Column index \(index) out of bounds")
		}

		let stmt = statement.stmt
		let idx = Int32(index)
		switch sqlite3_column_type(stmt, idx) {
		case SQLITE_NULL:
			return nil
		default:
			return try T(stmt, column: idx)
		}
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - throws: An error if `index` is out of bounds or the column contains a null or illegal value
	///
	/// - returns: The column's value
	public func value<T: ColumnConvertible>(at index: Int) throws -> T {
		guard index >= 0, index < self.columnCount else {
			throw DatabaseError("Column index \(index) out of bounds")
		}

		let stmt = statement.stmt
		let idx = Int32(index)
		switch sqlite3_column_type(stmt, idx) {
		case SQLITE_NULL:
			throw DatabaseError("Database null encountered at column \(index)")
		default:
			return try T(stmt, column: idx)
		}
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - throws: An error if the column doesn't exist or contains an illegal value
	///
	/// - returns: The column's value or `nil` if null
	public func value<T: ColumnConvertible>(named name: String) throws -> T? {
		return try value(at: statement.index(ofColumn: name))
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - throws: An error if the column doesn't exist or contains a null or illegal value
	///
	/// - returns: The column's value
	public func value<T: ColumnConvertible>(named name: String) throws -> T {
		return try value(at: statement.index(ofColumn: name))
	}

	public subscript<T: ColumnConvertible>(at index: Int) -> T? {
		return try? value(at: index)
	}

	public subscript<T: ColumnConvertible>(named name: String) -> T? {
		return try? value(named: name)
	}
}

extension Row {
	/// Returns the value of the leftmost column.
	///
	/// This is a shortcut for `value(at: 0)`.
	///
	/// - throws: An error if there are no columns or the column contains a null or illegal value
	///
	/// - returns: The column's value
	public func leftmostValue<T: ColumnConvertible>() throws -> T {
		return try value(at: 0)
	}
}

extension Statement {
	/// Returns the value of the leftmost column in the first row.
	///
	/// - throws: An error if there are no columns or the column contains an illegal value
	///
	/// - returns: The value of the leftmost column in the first row
	public func front<T: ColumnConvertible>() throws -> T? {
		return try firstRow()?.value(at: 0)
	}

	/// Returns the value of the leftmost column in the first row.
	///
	/// - throws: An error if there are no rows, no columns, or the column contains a null or illegal value
	///
	/// - returns: The value of the leftmost column in the first row
	public func front<T: ColumnConvertible>() throws -> T {
		guard let row = try firstRow() else {
			throw DatabaseError("Statement returned no rows")
		}
		return try row.value(at: 0)
	}
}

extension String: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(cString: sqlite3_column_text(stmt, idx))
	}
}

extension Data: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		let byteCount = Int(sqlite3_column_bytes(stmt, idx))
		self.init(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
	}
}

extension Int: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(bitPattern: Int(sqlite3_column_int64(stmt, idx)))
	}
}

extension Int8: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt8: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int16: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt16: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int32: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt32: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int64: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self = sqlite3_column_int64(stmt, idx)
	}
}

extension UInt64: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(bitPattern: sqlite3_column_int64(stmt, idx))
	}
}

extension Float: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_double(stmt, idx))
	}
}

extension Double: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self = sqlite3_column_double(stmt, idx)
	}
}

extension Bool: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx) != 0)
	}
}

extension UUID: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let u = UUID(uuidString: s) else {
			throw DatabaseError("String \"\(s)\" isn't a valid UUID")
		}
		self = u
	}
}

extension URL: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let u = URL(string: s) else {
			throw DatabaseError("String \"\(s)\" isn't a valid URL")
		}

		self = u
	}
}

extension Date: ColumnConvertible {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let d = iso8601DateFormatter.date(from: s) else {
			throw DatabaseError("String \"\(s)\" isn't a valid ISO 8601 date")
		}

		self = d
	}
}

extension ColumnConvertible where Self: Decodable {
	public init(_ stmt: SQLitePreparedStatement, column idx: Int32) throws {
		let byteCount = Int(sqlite3_column_bytes(stmt, idx))
		let data = Data(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
		self = try JSONDecoder().decode(Self.self, from: data)
	}
}
