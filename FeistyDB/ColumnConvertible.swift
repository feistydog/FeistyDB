/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A type that can be initialized directly from a column in an SQLite result row.
///
/// The implementation should use one of the `sqlite_column_X()` functions documented at [Result Values From A Query](https://sqlite.org/c3ref/column_blob.html).
///
/// For example, the implementation for `Int64` is:
///
/// ```swift
/// extension Int64: ColumnConvertible {
///     public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
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
	init(with stmt: SQLitePreparedStatement, parameter idx: Int32) throws
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
//		guard index >= 0, index < self.columnCount else {
//			throw DatabaseError.sqliteError("Column index \(index) out of bounds")
//		}

		let stmt = statement.stmt
		let idx = Int32(index)
		switch sqlite3_column_type(stmt, idx) {
		case SQLITE_NULL:
			return nil
		default:
			return try T(with: stmt, parameter: idx)
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
//		guard index >= 0, index < self.columnCount else {
//			throw DatabaseError.sqliteError("Column index \(index) out of bounds")
//		}

		let stmt = statement.stmt
		let idx = Int32(index)
		switch sqlite3_column_type(stmt, idx) {
		case SQLITE_NULL:
			throw DatabaseError.dataFormatError("Database null encountered at column \(index)")
		default:
			return try T(with: stmt, parameter: idx)
		}
	}

	/// Returns the value of column `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - throws: An error if the column doesn't exist or contains an illegal value
	///
	/// - returns: The column's value or `nil` if null
	public func value<T: ColumnConvertible>(named name: String) throws -> T? {
		guard let index = statement.columnNamesAndIndexes[name] else {
			throw DatabaseError.sqliteError("Unknown column \"\(name)\"")
		}
		return try value(at: index)
	}

	/// Returns the value of column `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - throws: An error if the column doesn't exist or contains a null or illegal value
	///
	/// - returns: The column's value
	public func value<T: ColumnConvertible>(named name: String) throws -> T {
		guard let index = statement.columnNamesAndIndexes[name] else {
			throw DatabaseError.sqliteError("Unknown column \"\(name)\"")
		}
		return try value(at: index)
	}
}

extension String: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(cString: sqlite3_column_text(stmt, idx))
	}
}

extension Data: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		let byteCount = Int(sqlite3_column_bytes(stmt, idx))
		self.init(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
	}
}

extension Int: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(bitPattern: Int(sqlite3_column_int64(stmt, idx)))
	}
}

extension Int8: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt8: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int16: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt16: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int32: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt32: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int64: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self = sqlite3_column_int64(stmt, idx)
	}
}

extension UInt64: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(bitPattern: sqlite3_column_int64(stmt, idx))
	}
}

extension Float: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_double(stmt, idx))
	}
}

extension Double: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self = sqlite3_column_double(stmt, idx)
	}
}

extension Bool: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx) != 0)
	}
}

extension UUID: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let u = UUID(uuidString: s) else {
			throw DatabaseError.dataFormatError("String \"\(s)\" isn't a valid UUID")
		}
		self = u
	}
}

extension URL: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let u = URL(string: s) else {
			throw DatabaseError.dataFormatError("String \"\(s)\" isn't a valid URL")
		}

		self = u
	}
}

extension Date: ColumnConvertible {
	public init(with stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let d = iso8601DateFormatter.date(from: s) else {
			throw DatabaseError.dataFormatError("String \"\(s)\" isn't a valid ISO 8601 date")
		}

		self = d
	}
}
