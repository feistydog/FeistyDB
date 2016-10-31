/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// Protocol types may adopt for database storage
public protocol DatabaseValueConvertible {
	/// Convert `self` to a database value
	///
	/// - returns: A `DatabaseValue` representing `self`
	func toDatabaseValue() -> DatabaseValue

	/// Convert a database value to the type of `Self`
	///
	/// - parameter value: The database value to convert
	/// - returns: An instance of `Self` or `nil`
	static func fromDatabaseValue(_ value: DatabaseValue) -> Self?
}

/// Parameter binding for convertible types
extension Statement {
	/// Bind a value to a statement parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter index: The index of the desired parameter
	/// - throws: `DatabaseError`
	public func bind<T: DatabaseValueConvertible>(value: T, toParameter index: Int) throws {
		try bind(value: value.toDatabaseValue(), toParameter: index)
	}

	/// Bind a value to a statement parameter
	///
	/// Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	/// - parameter value: The desired value of the parameter
	/// - parameter name: The name of the desired parameter
	/// - throws: `DatabaseError`
	public func bind<T: DatabaseValueConvertible>(value: T, toParameter name: String) throws {
		try bind(value: value.toDatabaseValue(), toParameter: name)
	}

	/// Bind a sequence of values to statement parameters
	///
	/// - parameter values: A sequence of `DatabaseValueConvertible` instances to bind
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: DatabaseValueConvertible>(_ values: S) throws where S.Iterator.Element == Optional<T> {
		var index = 1
		for value in values {
			if let value = value {
				try bind(value: value.toDatabaseValue(), toParameter: index)
			}
			else {
				try bind(value: DatabaseValue.null, toParameter: index)
			}
			index += 1
		}
	}

	/// Bind a sequence of values to statement parameters
	///
	/// - parameter values: A sequence of `DatabaseValueConvertible` instances to bind
	/// - throws: `DatabaseError`
	public func bind<S: Sequence, T: DatabaseValueConvertible>(_ values: S) throws where S.Iterator.Element == (String, Optional<T>) {
		for (key, value) in values {
			if let value = value {
				try bind(value: value.toDatabaseValue(), toParameter: key)
			}
			else {
				try bind(value: DatabaseValue.null, toParameter: key)
			}
		}
	}

}

/// Column values for convertible types
extension Row {
	/// Retrieve the value of the column
	///
	/// - returns: The column's value
	public func column<T: DatabaseValueConvertible>(_ index: Int) -> T? {
		return T.fromDatabaseValue(column(index))
	}
}

/// Column values for convertible types
extension Column {
	/// Retrieve the value of the column
	///
	/// - returns: The column's value
	public func value<T: DatabaseValueConvertible>() -> T? {
		return row.column(Int(idx))
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
