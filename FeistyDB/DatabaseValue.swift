/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// The native SQLite data types that may be stored in a database
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

/// `DatabaseValue` parameter binding
extension Statement {
	/// Bind a value to a statement parameter
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

	/// Bind a value to a statement parameter
	///
	/// - parameter value: The desired value of the parameter
	/// - parameter name: The name of the desired parameter
	/// - throws: `DatabaseError`
	public func bind(value: DatabaseValue, toParameter name: String) throws {
		let index = Int(sqlite3_bind_parameter_index(stmt, name))
		try bind(value: value, toParameter: index)
	}

	/// Bind a sequence of values to statement parameters
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

	/// Bind a sequence of values to statement parameters
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
	/// Retrieve a single column's value from the row
	///
	/// - parameter index: The 0-based index of the desired column
	/// - returns: The column's value
	public func column(_ index: Int) -> DatabaseValue {
//		precondition(index >= 0, "Column indexes are 0-based")
//		precondition(index < self.columnCount, "Column index out of bounds")

		let idx = Int32(index)
		switch sqlite3_column_type(stmt, idx) {
		case SQLITE_INTEGER:
			return DatabaseValue.integer(sqlite3_column_int64(stmt, idx))

		case SQLITE_FLOAT:
			return DatabaseValue.float(sqlite3_column_double(stmt, idx))

		case SQLITE_TEXT:
			let byteCount = Int(sqlite3_column_bytes(stmt, idx))
			let data = Data(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
			guard let string = String(data: data, encoding: .utf8) else {
				#if DEBUG
					print("Error converting data to UTF-8")
				#endif
				return DatabaseValue.null
			}
			return DatabaseValue.text(string)

		case SQLITE_BLOB:
			let byteCount = Int(sqlite3_column_bytes(stmt, idx))
			let data = Data(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
			return DatabaseValue.blob(data)

		default:
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
		return row.column(Int(idx))
	}
}
