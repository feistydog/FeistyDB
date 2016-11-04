/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A type that may be serialized to and from a database.
///
/// For example, the implementation for `NSNull` is:
///
/// ```swift
/// extension NSNull: DatabaseValueConvertible {
///     public func databaseValue() -> DatabaseValue {
///         return DatabaseValue.null
///     }
///
///     public static func from(databaseValue value: DatabaseValue) throws -> Self {
///         switch value {
///         case .null:
///             return self.init()
///         default:
///             throw DatabaseError.dataFormatError("DatabaseValue \"\(value)\" is not null")
///         }
///     }
/// }
/// ```
public protocol DatabaseValueConvertible: ParameterBindable {
	/// Returns the value of `self` as a serializable value.
	///
	/// - returns: A serializable instance representing `self`
	func databaseValue() -> DatabaseValue

	/// Returns the value of `value` as the type of `Self`.
	///
	/// - parameter value: The serializable value to convert
	/// - returns: An instance of `Self`
	///
	/// - throws: An error if `value` contains an illegal value for `Self`
	static func from(databaseValue value: DatabaseValue) throws -> Self
}

extension DatabaseValueConvertible {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		try databaseValue().bind(to: stmt, parameter: idx)
	}
}

extension Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - precondition: `index >= 0`
	/// - precondition: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - returns: The column's value
	///
	/// - throws: An error if the column contains an illegal value
	public func column<T: DatabaseValueConvertible>(_ index: Int) throws -> T {
		return try T.from(databaseValue: column(index))
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - returns: The column's value
	///
	/// - throws: An error if the column wasn't found or contains an illegal value
	public func column<T: DatabaseValueConvertible>(_ name: String) throws -> T {
		return try T.from(databaseValue: column(name))
	}
}

extension Column {
	/// Returns the value of the column.
	///
	/// - returns: The column's value
	///
	/// - throws: An error if the column contains an illegal value
	public func value<T: DatabaseValueConvertible>() throws -> T {
		return try row.column(index)
	}
}

extension NSNumber: DatabaseValueConvertible {
	public func databaseValue() -> DatabaseValue {
		switch CFNumberGetType(self as CFNumber) {
		case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
		     .sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
			return DatabaseValue.integer(self.int64Value)

		case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
			return DatabaseValue.float(self.doubleValue)
		}
	}

	public static func from(databaseValue value: DatabaseValue) throws -> Self {
		switch value {
		case .integer(let i):
			return self.init(value: i)
		case .float(let f):
			return self.init(value: f)
		default:
			throw DatabaseError.dataFormatError("\(value) is not a number")
		}
	}
}

extension NSNull: DatabaseValueConvertible {
	public func databaseValue() -> DatabaseValue {
		return DatabaseValue.null
	}

	public static func from(databaseValue value: DatabaseValue) throws -> Self {
		switch value {
		case .null:
			return self.init()
		default:
			throw DatabaseError.dataFormatError("\(value) is not null")
		}
	}
}
