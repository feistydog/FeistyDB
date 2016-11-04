/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A type that can bind their value directly to a parameter in an SQLite statement for efficiency.
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
	/// Binds the value of `self` to the SQL parameter at `idx` in `stmt`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - precondition: `index > 0`
	/// - precondition: `index < parameterCount`
	///
	/// - parameter stmt: An `sqlite3_stmt *` object
	/// - parameter idx: The index of the SQL parameter to bind
	///
	/// - throws: An error if `idx` is out of bounds or `self` couldn't be bound
	func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws
}

extension Database {
	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A series of values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func execute<T: ParameterBindable>(sql: String, parameters values: T...) throws {
		try execute(sql: sql, parameters: values)
	}

	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A series of values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func execute<T: ParameterBindable>(sql: String, parameters values: T?...) throws {
		try execute(sql: sql, parameters: values)
	}

	/// Executes `stmt` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter parameters: A dictionary of names and values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func execute<T: ParameterBindable>(sql: String, parameters: [String: T]) throws {
		try execute(sql: sql, parameters: parameters)
	}

	/// Executes `stmt` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter parameters: A dictionary of names and values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func execute<T: ParameterBindable>(sql: String, parameters: [String: T?]) throws {
		try execute(sql: sql, parameters: parameters)
	}

	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: ParameterBindable>(sql: String, parameters values: S) throws where S.Iterator.Element == T {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: values)
		try statement.execute()
	}

	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: ParameterBindable>(sql: String, parameters values: S) throws where S.Iterator.Element == T? {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: values)
		try statement.execute()
	}

	/// Executes `stmt` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: ParameterBindable>(sql: String, parameters: S) throws where S.Iterator.Element == (String, T) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: parameters)
		try statement.execute()
	}

	/// Executes `stmt` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: ParameterBindable>(sql: String, parameters: S) throws where S.Iterator.Element == (String, T?) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: parameters)
		try statement.execute()
	}
}

extension Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - precondition: `index > 0`
	/// - precondition: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter
	/// - parameter index: The index of the SQL parameter to bind
	///
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(value: T, toParameter index: Int) throws {
		try value.bind(to: stmt, parameter: Int32(index))
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - precondition: `index > 0`
	/// - precondition: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter
	/// - parameter index: The index of the SQL parameter to bind
	///
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

	/// Binds `value` to SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter
	/// - parameter name: The name of the SQL parameter to bind
	///
	/// - throws: An error if the parameter doesn't exist or couldn't be bound
	public func bind<T: ParameterBindable>(value: T, toParameter name: String) throws {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		guard idx > 0 else {
			throw DatabaseError.sqliteError("Unknown parameter \"\(name)\"")
		}
		try value.bind(to: stmt, parameter: idx)
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter
	/// - parameter name: The name of the SQL parameter to bind
	///
	/// - throws: An error if the parameter doesn't exist or couldn't be bound
	public func bind<T: ParameterBindable>(value: T?, toParameter name: String) throws {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		guard idx > 0 else {
			throw DatabaseError.sqliteError("Unknown parameter \"\(name)\"")
		}

		if let value = value {
			try value.bind(to: stmt, parameter: idx)
		}
		else {
			sqlite3_bind_null(stmt, idx)
		}
	}

	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - parameter values: A series of values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(parameters values: T...) throws  {
		try bind(parameters: values)
	}

	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - parameter values: A series of values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(parameters values: T?...) throws  {
		try bind(parameters: values)
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(parameters: [String: T]) throws  {
		try bind(parameters: parameters)
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func bind<T: ParameterBindable>(parameters: [String: T?]) throws  {
		try bind(parameters: parameters)
	}

	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - parameter values: A sequence of values to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: ParameterBindable>(parameters values: S) throws where S.Iterator.Element == T {
		var index: Int32 = 1
		for value in values {
			try value.bind(to: stmt, parameter: index)
			index += 1
		}
	}

	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - parameter values: A sequence of values to bind to SQL parameters
	///
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

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: ParameterBindable>(parameters: S) throws where S.Iterator.Element == (String, T) {
		for (name, value) in parameters {
			let idx = sqlite3_bind_parameter_index(stmt, name)
			guard idx > 0 else {
				throw DatabaseError.sqliteError("Unknown parameter \"\(name)\"")
			}
			try value.bind(to: stmt, parameter: idx)
		}
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: ParameterBindable>(parameters: S) throws where S.Iterator.Element == (String, T?) {
		for (name, value) in parameters {
			let idx = sqlite3_bind_parameter_index(stmt, name)
			guard idx > 0 else {
				throw DatabaseError.sqliteError("Unknown parameter \"\(name)\"")
			}
			if let value = value {
				try value.bind(to: stmt, parameter: idx)
			}
			else {
				sqlite3_bind_null(stmt, idx)
			}
		}
	}
}

extension DatabaseValue: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		switch self {
		case .integer(let i):
			if sqlite3_bind_int64(stmt, idx, i) != SQLITE_OK {
				throw DatabaseError.sqliteError("Error binding Int64 \(i) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			}

		case .float(let f):
			if sqlite3_bind_double(stmt, idx, f) != SQLITE_OK {
				throw DatabaseError.sqliteError("Error binding Double \(f) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			}

		case .text(let t):
			if sqlite3_bind_text(stmt, idx, t, -1, SQLITE_TRANSIENT) != SQLITE_OK {
				throw DatabaseError.sqliteError("Error binding string \"\(t)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			}

		case .blob(let b):
			try b.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) throws in
				guard sqlite3_bind_blob(stmt, idx, bytes, Int32(b.count), SQLITE_TRANSIENT) == SQLITE_OK else {
					throw DatabaseError.sqliteError("Error binding Data to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
				}
			}

		case .null:
			if sqlite3_bind_null(stmt, idx) != SQLITE_OK {
				throw DatabaseError.sqliteError("Error binding null to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			}
		}
		
	}
}

extension String: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding String \"\(self)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension Data: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		try self.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) throws in
			guard sqlite3_bind_blob(stmt, idx, bytes, Int32(self.count), SQLITE_TRANSIENT) == SQLITE_OK else {
				throw DatabaseError.sqliteError("Error binding Data to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
			}
		}
	}
}

extension Int: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding Int \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension UInt: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(Int(bitPattern: self))) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding UInt \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension Int8: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding Int8 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension UInt8: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding UInt8 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension Int16: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding Int16 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension UInt16: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding UInt16 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension Int32: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding Int32 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension UInt32: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding UInt32 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension Int64: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, self) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding Int64 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension UInt64: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(bitPattern: self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding UInt64 \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension Float: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_double(stmt, idx, Double(self)) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding Float \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension Double: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_double(stmt, idx, self) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding Double \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension Bool: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, self ? 1 : 0) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding Bool \(self) to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension UUID: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self.uuidString, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding UUID \"\(self)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}

extension URL: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self.absoluteString, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			throw DatabaseError.sqliteError("Error binding URL \"\(self)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
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
			throw DatabaseError.sqliteError("Error binding Date \"\(self)\" to parameter \(idx): \(String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt))))")
		}
	}
}
