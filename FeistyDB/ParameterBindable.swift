/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A type that can bind its value directly to a parameter in an SQLite statement.
///
/// The implementation should use one of the `sqlite_bind_X()` functions documented at [Binding Values To Prepared Statements](https://sqlite.org/c3ref/bind_blob.html).
///
/// For example, the implementation for `Int64` is:
///
/// ```swift
/// extension Int64: ParameterBindable {
///     public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
///         guard sqlite3_bind_int64(stmt, idx, self) == SQLITE_OK else {
///             throw DatabaseError(message: "Error binding Int64 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
///         }
///     }
/// }
/// ```
public protocol ParameterBindable {
	/// Binds the value of `self` to the SQL parameter at `idx` in `stmt`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - requires: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter stmt: An `sqlite3_stmt *` object
	/// - parameter idx: The index of the SQL parameter to bind
	///
	/// - throws: An error if `idx` is out of bounds or `self` couldn't be bound
	func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws
}

extension Database {
	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A series of values to bind to SQL parameters
	/// - parameter block: A closure called for each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed
	public func execute(sql: String, parameterValues values: ParameterBindable..., _ block: ((_ row: Row) throws -> ())? = nil) throws {
		try execute(sql: sql, parameterValues: values, block)
	}

	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A series of values to bind to SQL parameters
	/// - parameter block: A closure called for each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed
	public func execute(sql: String, parameterValues values: ParameterBindable?..., _ block: ((_ row: Row) throws -> ())? = nil) throws {
		try execute(sql: sql, parameterValues: values, block)
	}
}

extension Database {
	// The first two functions cause a compiler crash 😞

//	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt` and applies `block` to each result row.
//	///
//	/// - parameter sql: The SQL statement to execute
//	/// - parameter values: A series of values to bind to SQL parameters
//	/// - parameter block: A closure called for each result row
//	/// - parameter row: A result row of returned data
//	///
//	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed
//	public func execute(sql: String, parameterValues values: [ParameterBindable], _ block: ((_ row: Row) throws -> ())? = nil) throws {
//		let statement = try prepare(sql: sql)
//		try statement.bind(parameterValues: values)
//		if let block = block {
//			try statement.execute(block)
//		}
//		else {
//			try statement.execute()
//		}
//	}
//
//	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt` and applies `block` to each result row.
//	///
//	/// - parameter sql: The SQL statement to execute
//	/// - parameter values: A series of values to bind to SQL parameters
//	/// - parameter block: A closure called for each result row
//	/// - parameter row: A result row of returned data
//	///
//	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed
//	public func execute(sql: String, parameterValues values: [ParameterBindable?], _ block: ((_ row: Row) throws -> ())? = nil) throws {
//		let statement = try prepare(sql: sql)
//		try statement.bind(parameterValues: values)
//		if let block = block {
//			try statement.execute(block)
//		}
//		else {
//			try statement.execute()
//		}
//	}

	/// Executes `stmt` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter parameters: A dictionary of names and values to bind to SQL parameters
	/// - parameter block: A closure called for each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `parameters` couldn't be bound, or the statement couldn't be executed
	public func execute(sql: String, parameters: [String: ParameterBindable], _ block: ((_ row: Row) throws -> ())? = nil) throws {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: parameters)
		if let block = block {
			try statement.execute(block)
		}
		else {
			try statement.execute()
		}
	}

	/// Executes `stmt` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter parameters: A dictionary of names and values to bind to SQL parameters
	/// - parameter block: A closure called for each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `parameters` couldn't be bound, or the statement couldn't be executed
	public func execute(sql: String, parameters: [String: ParameterBindable?], _ block: ((_ row: Row) throws -> ())? = nil) throws {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: parameters)
		if let block = block {
			try statement.execute(block)
		}
		else {
			try statement.execute()
		}
	}
}

extension Database {
	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of values to bind to SQL parameters
	/// - parameter block: A closure called for each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed
	public func execute<S: Sequence>(sql: String, parameterValues values: S, _ block: ((_ row: Row) throws -> ())? = nil) throws where S.Iterator.Element == ParameterBindable {
		let statement = try prepare(sql: sql)
		try statement.bind(parameterValues: values)
		if let block = block {
			try statement.execute(block)
		}
		else {
			try statement.execute()
		}
	}

	/// Executes `stmt` with the *n* parameters in `values` bound to the first *n* SQL parameters of `stmt` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of values to bind to SQL parameters
	/// - parameter block: A closure called for each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed
	public func execute<S: Sequence>(sql: String, parameterValues values: S, _ block: ((_ row: Row) throws -> ())? = nil) throws where S.Iterator.Element == ParameterBindable? {
		let statement = try prepare(sql: sql)
		try statement.bind(parameterValues: values)
		if let block = block {
			try statement.execute(block)
		}
		else {
			try statement.execute()
		}
	}

	/// Executes `stmt` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	/// - parameter block: A closure called for each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `parameters` couldn't be bound, or the statement couldn't be executed
	public func execute<S: Sequence>(sql: String, parameters: S, _ block: ((_ row: Row) throws -> ())? = nil) throws where S.Iterator.Element == (String, ParameterBindable) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: parameters)
		if let block = block {
			try statement.execute(block)
		}
		else {
			try statement.execute()
		}
	}

	/// Executes `stmt` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	/// - parameter block: A closure called for each result row
	/// - parameter row: A result row of returned data
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `parameters` couldn't be bound, or the statement couldn't be executed
	public func execute<S: Sequence>(sql: String, parameters: S, _ block: ((_ row: Row) throws -> ())? = nil) throws where S.Iterator.Element == (String, ParameterBindable?) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: parameters)
		if let block = block {
			try statement.execute(block)
		}
		else {
			try statement.execute()
		}
	}
}

extension Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - requires: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter
	/// - parameter index: The index of the SQL parameter to bind
	///
	/// - throws: An error if `value` couldn't be bound
	public func bind<T: ParameterBindable>(value: T, toParameter index: Int) throws {
		let idx = Int32(index)
		try value.bind(to: stmt, parameter: idx)
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - requires: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter
	/// - parameter index: The index of the SQL parameter to bind
	///
	/// - throws: An error if `value` couldn't be bound
	public func bind<T: ParameterBindable>(value: T?, toParameter index: Int) throws {
		let idx = Int32(index)
		if let value = value {
			try value.bind(to: stmt, parameter: idx)
		}
		else {
			guard sqlite3_bind_null(stmt, idx) == SQLITE_OK else {
				throw DatabaseError(message: "Error binding null to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}
		}
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter
	/// - parameter name: The name of the SQL parameter to bind
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound
	public func bind<T: ParameterBindable>(value: T, toParameter name: String) throws {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		guard idx > 0 else {
			throw DatabaseError("Unknown parameter \"\(name)\"")
		}

		try value.bind(to: stmt, parameter: idx)
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter
	/// - parameter name: The name of the SQL parameter to bind
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound
	public func bind<T: ParameterBindable>(value: T?, toParameter name: String) throws {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		guard idx > 0 else {
			throw DatabaseError("Unknown parameter \"\(name)\"")
		}

		if let value = value {
			try value.bind(to: stmt, parameter: idx)
		}
		else {
			guard sqlite3_bind_null(stmt, idx) == SQLITE_OK else {
				throw DatabaseError(message: "Error binding null to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}
		}
	}
}

extension Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - parameter values: A series of values to bind to SQL parameters
	///
	/// - throws: An error if one of `values` couldn't be bound
	public func bind(parameterValues values: ParameterBindable...) throws  {
		try bind(parameterValues: values)
	}

	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - parameter values: A series of values to bind to SQL parameters
	///
	/// - throws: An error if one of `values` couldn't be bound
	public func bind(parameterValues values: ParameterBindable?...) throws  {
		try bind(parameterValues: values)
	}
}

extension Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - requires: `values.count <= self.parameterCount`
	///
	/// - parameter values: A series of values to bind to SQL parameters
	///
	/// - throws: An error if one of `values` couldn't be bound
	public func bind(parameterValues values: [ParameterBindable]) throws  {
		var index: Int32 = 1
		for value in values {
			try value.bind(to: stmt, parameter: index)
			index += 1
		}
	}

	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - requires: `values.count <= self.parameterCount`
	///
	/// - parameter values: A series of values to bind to SQL parameters
	///
	/// - throws: An error if one of `values` couldn't be bound
	public func bind(parameterValues values: [ParameterBindable?]) throws  {
		var index: Int32 = 1
		for value in values {
			if let value = value {
				try value.bind(to: stmt, parameter: index)
			}
			else {
				guard sqlite3_bind_null(stmt, index) == SQLITE_OK else {
					throw DatabaseError(message: "Error binding null to parameter \(index)", takingDescriptionFromStatement: stmt)
				}
			}
			index += 1
		}
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound
	public func bind(parameters: [String: ParameterBindable]) throws  {
		for (name, value) in parameters {
			let index = sqlite3_bind_parameter_index(stmt, name)
			guard index > 0 else {
				throw DatabaseError("Unknown parameter \"\(name)\"")
			}
			try value.bind(to: stmt, parameter: index)
		}
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound
	public func bind(parameters: [String: ParameterBindable?]) throws  {
		for (name, value) in parameters {
			let index = sqlite3_bind_parameter_index(stmt, name)
			guard index > 0 else {
				throw DatabaseError("Unknown parameter \"\(name)\"")
			}
			if let value = value {
				try value.bind(to: stmt, parameter: index)
			}
			else {
				guard sqlite3_bind_null(stmt, index) == SQLITE_OK else {
					throw DatabaseError(message: "Error binding null to parameter \(index)", takingDescriptionFromStatement: stmt)
				}
			}
		}
	}
}

extension Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - parameter values: A sequence of values to bind to SQL parameters
	///
	/// - throws: An error if one of `values` couldn't be bound
	public func bind<S: Sequence>(parameterValues values: S) throws where S.Iterator.Element == ParameterBindable {
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
	/// - throws: An error if one of `values` couldn't be bound
	public func bind<S: Sequence>(parameterValues values: S) throws where S.Iterator.Element == ParameterBindable? {
		var index: Int32 = 1
		for value in values {
			if let value = value {
				try value.bind(to: stmt, parameter: index)
			}
			else {
				guard sqlite3_bind_null(stmt, index) == SQLITE_OK else {
					throw DatabaseError(message: "Error binding null to parameter \(index)", takingDescriptionFromStatement: stmt)
				}
			}
			index += 1
		}
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound
	public func bind<S: Sequence>(parameters: S) throws where S.Iterator.Element == (String, ParameterBindable) {
		for (name, value) in parameters {
			let index = sqlite3_bind_parameter_index(stmt, name)
			guard index > 0 else {
				throw DatabaseError("Unknown parameter \"\(name)\"")
			}
			try value.bind(to: stmt, parameter: index)
		}
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A sequence of name and value pairs to bind to SQL parameters
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound
	public func bind<S: Sequence>(parameters: S) throws where S.Iterator.Element == (String, ParameterBindable?) {
		for (name, value) in parameters {
			let index = sqlite3_bind_parameter_index(stmt, name)
			guard index > 0 else {
				throw DatabaseError("Unknown parameter \"\(name)\"")
			}
			if let value = value {
				try value.bind(to: stmt, parameter: index)
			}
			else {
				guard sqlite3_bind_null(stmt, index) == SQLITE_OK else {
					throw DatabaseError(message: "Error binding null to parameter \(index)", takingDescriptionFromStatement: stmt)
				}
			}
		}
	}
}

extension DatabaseValue: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		switch self {
		case .integer(let i):
			guard sqlite3_bind_int64(stmt, idx, i) == SQLITE_OK else {
				throw DatabaseError(message: "Error binding Int64 \(i) to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}

		case .float(let f):
			guard sqlite3_bind_double(stmt, idx, f) == SQLITE_OK else {
				throw DatabaseError(message: "Error binding Double \(f) to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}

		case .text(let t):
			guard sqlite3_bind_text(stmt, idx, t, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
				throw DatabaseError(message: "Error binding string \"\(t)\" to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}

		case .blob(let b):
			try b.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) throws in
				guard sqlite3_bind_blob(stmt, idx, bytes, Int32(b.count), SQLITE_TRANSIENT) == SQLITE_OK else {
					throw DatabaseError(message: "Error binding Data to parameter \(idx)", takingDescriptionFromStatement: stmt)
				}
			}

		case .null:
			guard sqlite3_bind_null(stmt, idx) == SQLITE_OK else {
				throw DatabaseError(message: "Error binding null to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}
		}
		
	}
}

extension String: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding String \"\(self)\" to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension Data: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		try self.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) throws in
			guard sqlite3_bind_blob(stmt, idx, bytes, Int32(self.count), SQLITE_TRANSIENT) == SQLITE_OK else {
				throw DatabaseError(message: "Error binding Data to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}
		}
	}
}

extension Int: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding Int \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension UInt: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(Int(bitPattern: self))) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding UInt \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension Int8: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding Int8 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension UInt8: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding UInt8 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension Int16: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding Int16 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension UInt16: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding UInt16 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension Int32: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding Int32 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension UInt32: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding UInt32 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension Int64: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, self) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding Int64 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension UInt64: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, Int64(bitPattern: self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding UInt64 \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension Float: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_double(stmt, idx, Double(self)) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding Float \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension Double: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_double(stmt, idx, self) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding Double \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension Bool: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_int64(stmt, idx, self ? 1 : 0) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding Bool \(self) to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension UUID: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self.uuidString, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding UUID \"\(self)\" to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}

extension URL: ParameterBindable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		guard sqlite3_bind_text(stmt, idx, self.absoluteString, -1, SQLITE_TRANSIENT) == SQLITE_OK else {
			throw DatabaseError(message: "Error binding URL \"\(self)\" to parameter \(idx)", takingDescriptionFromStatement: stmt)
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
			throw DatabaseError(message: "Error binding Date \"\(self)\" to parameter \(idx)", takingDescriptionFromStatement: stmt)
		}
	}
}
