/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// The native data types that may be stored in a database
///
/// - seealso: [Datatypes In SQLite Version 3](https://sqlite.org/datatype3.html)
public enum DatabaseValue {
	/// An integer value
	case integer(Int64)
	/// A floating-point value
	case float(Double)
	/// A text value
	case text(String)
	/// A blob (untyped bytes) value
	case blob(Data)
	/// A null value
	case null
}

/// An `sqlite3_value *` object
///
/// - seealso: [Obtaining SQL Values](https://sqlite.org/c3ref/value_blob.html)
typealias SQLiteValue = OpaquePointer

/// DatabaseValue initialization from SQLite values
extension DatabaseValue {
	/// Initialize `self` from an SQLite value
	///
	/// - parameter value: The desired value
	init(from value: SQLiteValue) {
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
				print("Unknown value type \(type) encountered")
			#endif
			self = .null
		}
	}
}

/// String conversion
extension DatabaseValue: CustomStringConvertible {
	/// A description of the type and value contained by this `DatabaseValue`
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

/// `DatabaseValue` parameter binding
extension Statement {
	/// Bind a value to an SQL parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter index: The index of the desired parameter
	/// - throws: `DatabaseError`
	public func bind(value: DatabaseValue, toParameter index: Int) throws {
//		precondition(index > 0, "Parameter indexes are 1-based")
//		precondition(index < self.parameterCount, "Parameter index out of bounds")

		switch value {
		case .integer(let i):
			if sqlite3_bind_int64(stmt, Int32(index), i) != SQLITE_OK {
				#if DEBUG
					print("Error binding Int64 \(i) to parameter \(index): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
			}

		case .float(let f):
			if sqlite3_bind_double(stmt, Int32(index), f) != SQLITE_OK {
				#if DEBUG
					print("Error binding Double \(f) to parameter \(index): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
			}

		case .text(let t):
			if sqlite3_bind_text(stmt, Int32(index), t, -1, SQLITE_TRANSIENT) != SQLITE_OK {
				#if DEBUG
					print("Error binding string \"\(t)\" to parameter \(index): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
			}

		case .blob(let b):
			try b.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) throws in
				guard sqlite3_bind_blob(stmt, Int32(index), bytes, Int32(b.count), SQLITE_TRANSIENT) == SQLITE_OK else {
					#if DEBUG
						print("Error binding Data to parameter \(index): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
					#endif
					throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
				}
			}

		case .null:
			if sqlite3_bind_null(stmt, Int32(index)) != SQLITE_OK {
				#if DEBUG
					print("Error binding null to parameter \(index): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
			}
		}

	}

	/// Bind a value to an SQL parameter
	///
	/// - parameter value: The desired value of the parameter
	/// - parameter name: The name of the desired parameter
	/// - throws: `DatabaseError`
	public func bind(value: DatabaseValue, toParameter name: String) throws {
		let index = Int(sqlite3_bind_parameter_index(stmt, name))
		try bind(value: value, toParameter: index)
	}

	/// Bind a sequence of values to SQL parameters
	///
	/// - parameter values: A sequence of `DatabaseValue` instances to bind
	/// - throws: `DatabaseError`
	public func bind<S: Sequence>(_ values: S) throws where S.Iterator.Element == DatabaseValue {
		var index = 1
		for value in values {
			try bind(value: value, toParameter: index)
			index += 1
		}
	}

	/// Bind a sequence of values to SQL parameters
	///
	/// - parameter values: A sequence of `DatabaseValue` instances to bind
	/// - throws: `DatabaseError`
	public func bind<S: Sequence>(_ values: S) throws where S.Iterator.Element == (String, DatabaseValue) {
		for (key, value) in values {
			try bind(value: value, toParameter: key)
		}
	}
}

/// `DatabaseValue` value retrieval
extension Row {
	/// Retrieve a column's value
	///
	/// - parameter index: The 0-based index of the desired column
	/// - returns: The column's value
	public func column(_ index: Int) -> DatabaseValue {
//		precondition(index >= 0, "Column indexes are 0-based")
//		precondition(index < self.columnCount, "Column index out of bounds")

		let stmt = statement.stmt
		let idx = Int32(index)
		
		let type = sqlite3_column_type(stmt, idx)
		switch type {
		case SQLITE_INTEGER:
			return DatabaseValue.integer(sqlite3_column_int64(stmt, idx))

		case SQLITE_FLOAT:
			return DatabaseValue.float(sqlite3_column_double(stmt, idx))

		case SQLITE_TEXT:
			return DatabaseValue.text(String(cString: sqlite3_column_text(stmt, idx)))

		case SQLITE_BLOB:
			let byteCount = Int(sqlite3_column_bytes(stmt, idx))
			let data = Data(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
			return DatabaseValue.blob(data)

		case SQLITE_NULL:
			return DatabaseValue.null

		default:
			#if DEBUG
				print("Unknown column type \(type) encountered")
			#endif
			return DatabaseValue.null
		}
	}
}

/// `DatabaseValue` value retrieval
extension Column {
	/// Retrieve the value of the column
	///
	/// - returns: The column's value
	public func value() -> DatabaseValue {
		return row.column(index)
	}
}
