/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A protocol allowing types to bind their value directly to an SQLite statement for efficiency
///
/// The implementation should use one of the `sqlite_bind_X()` functions documented at [Binding Values To Prepared Statements](https://sqlite.org/c3ref/bind_blob.html).
///
/// For example, the implementation for `Int64` is:
///
/// ```swift
/// extension Int64: ParameterBindable {
///     public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
///         guard sqlite3_bind_int64(stmt, idx, self) == SQLITE_OK else {
///            throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
///         }
///     }
/// }
/// ```
public protocol ParameterBindable {
	/// Bind the value of `self` to a parameter in an SQLite statement
	///
	/// - parameter stmt: An `sqlite3_stmt *` object
	/// - parameter idx: The index of the parameter to bind
	/// - throws: `DatabaseError`
	func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws
}

/// Convenience methods to execute SQL statements
extension Database {
	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A series of values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func execute<T: ParameterBindable>(sql: String, parameters values: T...) throws {
		try execute(sql: sql, parameters: values)
	}

	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A series of optional values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func execute<T: ParameterBindable>(sql: String, parameters values: T?...) throws {
		try execute(sql: sql, parameters: values)
	}

	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: ParameterBindable>(sql: String, parameters values: S) throws where S.Iterator.Element == T {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: values)
		try statement.execute()
	}

	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of optional values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: ParameterBindable>(sql: String, parameters values: S) throws where S.Iterator.Element == T? {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: values)
		try statement.execute()
	}

	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of name/value pairs to bind to named SQL parameters
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: ParameterBindable>(sql: String, parameters values: S) throws where S.Iterator.Element == (String, T) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: values)
		try statement.execute()
	}

	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of name/value pairs to bind to named SQL parameters
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: ParameterBindable>(sql: String, parameters values: S) throws where S.Iterator.Element == (String, T?) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: values)
		try statement.execute()
	}
}

/// Parameter binding for bindable types
extension Statement {
	/// Bind a value to an SQL parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter index: The index of the desired parameter
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(value: T, toParameter index: Int) throws {
		try value.bind(to: stmt, parameter: Int32(index))
	}

	/// Bind an optional value to an SQL parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter index: The index of the desired parameter
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(value: T?, toParameter index: Int) throws {
		let idx = Int32(index)
		if let value = value {
			try value.bind(to: stmt, parameter: idx)
		}
		else {
			sqlite3_bind_null(stmt, idx)
		}
	}

	/// Bind a value to a named SQL parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter name: The name of the desired parameter
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(value: T, toParameter name: String) throws {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		try value.bind(to: stmt, parameter: idx)
	}

	/// Bind an optional value to a named SQL parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter name: The name of the desired parameter
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(value: T?, toParameter name: String) throws {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		if let value = value {
			try value.bind(to: stmt, parameter: idx)
		}
		else {
			sqlite3_bind_null(stmt, idx)
		}
	}

	/// Bind a series of values to SQL parameters
	///
	/// - parameter values: A series of values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(parameters values: T...) throws  {
		try bind(parameters: values)
	}

	/// Bind a sequence of values to SQL parameters
	///
	/// - parameter values: A sequence of values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: ParameterBindable>(parameters values: S) throws where S.Iterator.Element == T {
		var index: Int32 = 1
		for value in values {
			try value.bind(to: stmt, parameter: index)
			index += 1
		}
	}

	/// Bind a sequence of optional values to SQL parameters
	///
	/// - parameter values: A sequence optional values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: ParameterBindable>(parameters values: S) throws where S.Iterator.Element == T? {
		var index: Int32 = 1
		for value in values {
			if let value = value {
				try value.bind(to: stmt, parameter: index)
			}
			else {
				sqlite3_bind_null(stmt, index)
			}
			index += 1
		}
	}

	/// Bind a sequence of values to named SQL parameters
	///
	/// - parameter values: A sequence of name/value pairs to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: ParameterBindable>(parameters values: S) throws where S.Iterator.Element == (String, T) {
		for (name, value) in values {
			let idx = sqlite3_bind_parameter_index(stmt, name)
			try value.bind(to: stmt, parameter: idx)
		}
	}

	/// Bind a sequence of optional values to named SQL parameters
	///
	/// - parameter values: A sequence of name/value pairs to bind to named SQL parameters
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: ParameterBindable>(parameters values: S) throws where S.Iterator.Element == (String, T?) {
		for (name, value) in values {
			let idx = sqlite3_bind_parameter_index(stmt, name)
			if let value = value {
				try value.bind(to: stmt, parameter: idx)
			}
			else {
				sqlite3_bind_null(stmt, idx)
			}
		}
	}
}

extension String: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			#if DEBUG
				print("Error binding String \"\(self)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension Data: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		try self.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) throws in
			guard sqlite3_bind_blob(stmt, idx, bytes, Int32(self.count), SQLITE_TRANSIENT) == SQLITE_OK else {
				#if DEBUG
					print("Error binding Data to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
				#endif
				throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
			}
		}
	}
}

extension Int: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Int \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension UInt: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(Int(bitPattern: self))) == SQLITE_OK else {
			#if DEBUG
				print("Error binding UInt \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension Int8: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Int8 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension UInt8: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding UInt8 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension Int16: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Int16 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension UInt16: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding UInt16 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension Int32: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Int32 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension UInt32: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding UInt32 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension Int64: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, self) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Int64 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension UInt64: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(bitPattern: self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding UInt64 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension Float: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_double(stmt, idx, Double(self)) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Float \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension Double: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_double(stmt, idx, self) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Double \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension Bool: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, self ? 1 : 0) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Bool \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension UUID: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self.uuidString, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			#if DEBUG
				print("Error binding UUID \"\(self)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

extension URL: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self.absoluteString, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			#if DEBUG
				print("Error binding URL \"\(self)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}

/// Used for date conversion
// FIXME: Thread safe when more than one Database exists?
let iso8601DateFormatter: ISO8601DateFormatter = {
	let dateFormatter = ISO8601DateFormatter()
	return dateFormatter
}()

extension Date: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, iso8601DateFormatter.string(from: self), -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			#if DEBUG
				print("Error binding Date \"\(self)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			#endif
			throw DatabaseError.sqliteError(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))
		}
	}
}
