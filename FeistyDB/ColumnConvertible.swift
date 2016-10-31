/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A protocol allowing types to initialize directly from an SQLite statement for efficiency
public protocol ColumnConvertible {
	/// Initialize `self` from an SQLite statement directly
	///
	/// Do not check for null database values
	/// - parameter stmt: An `sqlite3_stmt *` object
	/// - parameter index: The index of the desired parameter
	/// - throws: An error if initialization failed
	init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) throws
}

extension Row {
	/// Retrieve a column's value
	///
	/// - parameter index: The 0-based index of the desired column
	/// - returns: The column's value
	/// - throws: An error if the column contains an illegal value
	public func column<T: ColumnConvertible>(_ index: Int) throws -> T? {
		let stmt = statement.stmt
		let idx = Int32(index)
		switch sqlite3_column_type(stmt, idx) {
		case SQLITE_NULL:
			return nil
		default:
			return try T(withRawSQLiteStatement: stmt, parameter: idx)
		}
	}

	/// Retrieve a column's value
	///
	/// - parameter index: The 0-based index of the desired column
	/// - returns: The column's value
	/// - throws: An error if the column is null or contains an illegal value
	public func column<T: ColumnConvertible>(_ index: Int) throws -> T {
		let stmt = statement.stmt
		let idx = Int32(index)
		switch sqlite3_column_type(stmt, idx) {
		case SQLITE_NULL:
			throw DatabaseError.dataFormatError("Null encountered")
		default:
			return try T(withRawSQLiteStatement: stmt, parameter: idx)
		}
	}

	/// Retrieve a column's value
	///
	/// - parameter name: name of the desired column
	/// - returns: The column's value
	/// - throws: An error if the column doesn't exist or contains an illegal value
	public func column<T: ColumnConvertible>(_ name: String) throws -> T? {
		guard let index = statement.columnNamesAndIndexes[name] else {
			throw DatabaseError.sqliteError("Unknown column \"\(name)\"")
		}
		return try column(index)
	}

	/// Retrieve a column's value
	///
	/// - parameter name: name of the desired column
	/// - returns: The column's value
	/// - throws: An error if the column is null or contains an illegal value
	public func column<T: ColumnConvertible>(_ name: String) throws -> T {
		guard let index = statement.columnNamesAndIndexes[name] else {
			throw DatabaseError.sqliteError("Unknown column \"\(name)\"")
		}
		return try column(index)
	}
}

extension Column {
	/// Retrieve the value of the column
	///
	/// - returns: The column's value
	/// - throws: An error if the column contains an illegal value
	public func value<T: ColumnConvertible>() throws -> T? {
		return try row.column(index)
	}

	/// Retrieve the value of the column
	///
	/// - returns: The column's value
	/// - throws: An error if the column is null or contains an illegal value
	public func value<T: ColumnConvertible>() throws -> T {
		return try row.column(index)
	}
}

extension String: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(cString: sqlite3_column_text(stmt, idx))
	}
}

extension Data: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		let byteCount = Int(sqlite3_column_bytes(stmt, idx))
		self.init(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
	}
}

extension Int: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(bitPattern: Int(sqlite3_column_int64(stmt, idx)))
	}
}

extension Int8: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt8: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int16: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt16: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int32: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension UInt32: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx))
	}
}

extension Int64: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self = sqlite3_column_int64(stmt, idx)
	}
}

extension UInt64: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(bitPattern: sqlite3_column_int64(stmt, idx))
	}
}

extension Float: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_double(stmt, idx))
	}
}

extension Double: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self = sqlite3_column_double(stmt, idx)
	}
}

extension Bool: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) {
		self.init(sqlite3_column_int64(stmt, idx) != 0)
	}
}

extension UUID: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let u = UUID(uuidString: s) else {
			#if DEBUG
				print("String \"\(s)\" isn't a valid UUID")
			#endif
			throw DatabaseError.dataFormatError("String \"\(s)\" isn't a valid UUID")
		}
		self = u
	}
}

extension URL: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let u = URL(string: s) else {
			#if DEBUG
				print("String \"\(s)\" isn't a valid URL")
			#endif
			throw DatabaseError.dataFormatError("String \"\(s)\" isn't a valid URL")
		}

		self = u
	}
}

extension Date: ColumnConvertible {
	public init(withRawSQLiteStatement stmt: OpaquePointer, parameter idx: Int32) throws {
		let s = String(cString: sqlite3_column_text(stmt, idx))
		guard let d = iso8601DateFormatter.date(from: s) else {
			#if DEBUG
				print("String \"\(s)\" isn't a valid ISO 8601 date")
			#endif
			throw DatabaseError.dataFormatError("String \"\(s)\" isn't a valid ISO 8601 date")
		}

		self = d
	}
}
