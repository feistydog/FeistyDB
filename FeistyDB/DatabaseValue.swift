/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A native data type that may be stored in an SQLite database.
///
/// - seealso: [Datatypes In SQLite Version 3](https://sqlite.org/datatype3.html)
public enum DatabaseValue {
	/// An integer value.
	case integer(Int64)
	/// A floating-point value.
	case float(Double)
	/// A text value.
	case text(String)
	/// A blob (untyped bytes) value.
	case blob(Data)
	/// A null value.
	case null
}

/// An `sqlite3_value *` object.
///
/// - seealso: [Obtaining SQL Values](https://sqlite.org/c3ref/value_blob.html)
typealias SQLiteValue = OpaquePointer

extension DatabaseValue {
	/// Creates an instance containing `value`.
	///
	/// - parameter value: The desired value
	init(_ value: SQLiteValue) {
		let type = sqlite3_value_type(value)
		switch type {
		case SQLITE_INTEGER:
			self = .integer(sqlite3_value_int64(value))
		case SQLITE_FLOAT:
			self = .float(sqlite3_value_double(value))
		case SQLITE_TEXT:
			self = .text(String(cString: sqlite3_value_text(value)))
		case SQLITE_BLOB:
			self = .blob(Data(bytes: sqlite3_value_blob(value), count: Int(sqlite3_value_bytes(value))))
		case SQLITE_NULL:
			self = .null
		default:
			#if DEBUG
				print("Unknown SQLite value type \(type) encountered")
			#endif
			self = .null
		}
	}
}

extension DatabaseValue: CustomStringConvertible {
	/// A description of the type and value of `self`.
	public var description: String {
		switch self {
		case .integer(let i):
			return "DatabaseValue.integer(\(i))"
		case .float(let f):
			return "DatabaseValue.float(\(f))"
		case .text(let t):
			return "DatabaseValue.text(\(t))"
		case .blob(let b):
			return "DatabaseValue.blob(\(b))"
		case .null:
			return "DatabaseValue.null"
		}
	}
}

extension Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - returns: The column's value
	/// - throws: An error if the column doesn't exist
	public func value(at index: Int) throws -> DatabaseValue {
//		guard index >= 0, index < self.columnCount else {
//			throw DatabaseError.sqliteError("Column index \(index) out of bounds")
//		}

		let stmt = statement.stmt
		let idx = Int32(index)
		
		let type = sqlite3_column_type(stmt, idx)
		switch type {
		case SQLITE_INTEGER:
			return .integer(sqlite3_column_int64(stmt, idx))

		case SQLITE_FLOAT:
			return .float(sqlite3_column_double(stmt, idx))

		case SQLITE_TEXT:
			return .text(String(cString: sqlite3_column_text(stmt, idx)))

		case SQLITE_BLOB:
			let byteCount = Int(sqlite3_column_bytes(stmt, idx))
			let data = Data(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
			return .blob(data)

		case SQLITE_NULL:
			return .null

		default:
			#if DEBUG
				print("Unknown column type \(type) encountered")
			#endif
			return .null
		}
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - returns: The column's value
	///
	/// - throws: An error if the column doesn't exist
	public func value(named name: String) throws -> DatabaseValue {
		guard let index = statement.columnNamesAndIndexes[name] else {
			throw DatabaseError.sqliteError("Unknown column \"\(name)\"")
		}
		return try value(at: index)
	}
}

extension Row {
	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - returns: The column's value or `.null` if the column doesn't exist
	public subscript(name: String) -> DatabaseValue {
		do {
			return try value(named: name)
		}
		catch {
			return .null
		}
	}
}

extension Row: Collection {
	public var startIndex: Int {
		return 0
	}

	public var endIndex: Int {
		return statement.columnCount
	}

	public subscript(position: Int) -> DatabaseValue {
		do {
			return try value(at: position)
		}
		catch {
			return .null
		}
	}

	public func index(after i: Int) -> Int {
		return i + 1
	}
}
