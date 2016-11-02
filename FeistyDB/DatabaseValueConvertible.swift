/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// Protocol types may adopt for database storage
public protocol DatabaseValueConvertible {
	/// Convert `self` to a database value
	///
	/// - returns: A database value representing `self`
	func toDatabaseValue() -> DatabaseValue

	/// Convert a database value to the type of `Self`
	///
	/// - parameter value: The database value to convert
	/// - returns: An instance of `Self` or `nil`
	/// - throws: An error if the database value contains an illegal value
	static func fromDatabaseValue(_ value: DatabaseValue) throws -> Self?
}

/// Convenience methods to execute SQL statements
extension Database {
	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A series of values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func execute<T: DatabaseValueConvertible>(sql: String, parameters values: T...) throws {
		try execute(sql: sql, parameters: values)
	}

	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of values to bind to SQL parameters
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: DatabaseValueConvertible>(sql: String, parameters values: S) throws where S.Iterator.Element == T {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: values)
		try statement.execute()
	}

	/// Execute an SQL statement
	///
	/// - parameter sql: The SQL statement to execute
	/// - parameter values: A sequence of name/value pairs to bind to named SQL parameters
	/// - throws: `DatabaseError`
	public func execute<S: Sequence, T: DatabaseValueConvertible>(sql: String, parameters values: S) throws where S.Iterator.Element == (String, T) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: values)
		try statement.execute()
	}
}

/// Parameter binding for convertible types
extension Statement {
	/// Bind a value to an SQL parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter index: The index of the desired parameter
	/// - throws: `DatabaseError`
	public func bind<T: DatabaseValueConvertible>(value: T, toParameter index: Int) throws {
		try bind(value: value.toDatabaseValue(), toParameter: index)
	}

	/// Bind a value to a named SQL parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter name: The name of the desired parameter
	/// - throws: `DatabaseError`
	public func bind<T: DatabaseValueConvertible>(value: T, toParameter name: String) throws {
		try bind(value: value.toDatabaseValue(), toParameter: name)
	}

	/// Bind a series of values to SQL parameters
	///
	/// - parameter values: A series of `DatabaseValueConvertible` instances to bind
	/// - throws: `DatabaseError`
	public func bind<T: DatabaseValueConvertible>(parameters values: T...) throws {
		try bind(parameters: values)
	}

	/// Bind a sequence of values to SQL parameters
	///
	/// - parameter values: A sequence of `DatabaseValueConvertible` instances to bind
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: DatabaseValueConvertible>(parameters values: S) throws where S.Iterator.Element == T {
		var index = 1
		for value in values {
			try bind(value: value.toDatabaseValue(), toParameter: index)
			index += 1
		}
	}

	/// Bind a sequence of name/value pairs to named SQL parameters
	///
	/// - parameter values: A sequence of `DatabaseValueConvertible` instances to bind
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: DatabaseValueConvertible>(parameters values: S) throws where S.Iterator.Element == (String, T) {
		for (name, value) in values {
			try bind(value: value.toDatabaseValue(), toParameter: name)
		}
	}
}

/// Column values for convertible types
extension Row {
	/// Retrieve the value of the column
	///
	/// - returns: The column's value
	/// - throws: An error if the value contains an illegal value
	public func column<T: DatabaseValueConvertible>(_ index: Int) throws -> T? {
		return try T.fromDatabaseValue(column(index))
	}
}

/// Column values for convertible types
extension Column {
	/// Retrieve the value of the column
	///
	/// - returns: The column's value
	/// - throws: An error if the value contains an illegal value
	public func value<T: DatabaseValueConvertible>() throws -> T? {
		return try row.column(index)
	}
}

extension NSNumber: DatabaseValueConvertible {
	public func toDatabaseValue() -> DatabaseValue {
		switch CFNumberGetType(self as CFNumber) {
		case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
		     .sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
			return DatabaseValue.integer(self.int64Value)

		case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
			return DatabaseValue.float(self.doubleValue)
		}
	}

	public static func fromDatabaseValue(_ value: DatabaseValue) -> Self? {
		switch value {
		case .integer(let i):
			return self.init(value: i)
		case .float(let f):
			return self.init(value: f)
		default:
			return nil
		}
	}
}

extension NSNull: DatabaseValueConvertible {
	public func toDatabaseValue() -> DatabaseValue {
		return DatabaseValue.null
	}

	public static func fromDatabaseValue(_ value: DatabaseValue) throws -> Self? {
		switch value {
		case .null:
			return self.init()
		default:
			throw DatabaseError.dataFormatError("DatabaseValue \"\(value)\" is not null")
		}
	}
}
