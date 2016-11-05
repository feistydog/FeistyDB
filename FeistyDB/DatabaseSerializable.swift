/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A type that may be serialized to and deserialized from a database.
///
/// This is a more generic method for database storage than `ParameterBindable` and `ColumnConvertible`
/// because it allows types to customize behavior based on the database data type.
///
/// For example, the implementation for `NSNull` is:
///
/// ```swift
/// extension NSNull: DatabaseSerializable {
///     public func serialized() -> DatabaseValue {
///         return .null
///     }
///
///     public static func deserialize(from value: DatabaseValue) throws -> Self {
///         switch value {
///         case .null:
///             return self.init()
///         default:
///             throw DatabaseError.dataFormatError("\(value) is not null")
///         }
///     }
/// }
/// ```
public protocol DatabaseSerializable: ParameterBindable {
	/// Returns a serialized value of `self`.
	///
	/// - returns: A serialized value representing `self`
	func serialized() -> DatabaseValue

	/// Deserializes and returns `value` as `Self`.
	///
	/// - parameter value: A serialized value of `Self`
	/// - returns: An instance of `Self`
	///
	/// - throws: An error if `value` contains an illegal value for `Self`
	static func deserialize(from value: DatabaseValue) throws -> Self
}

extension DatabaseSerializable {
	public func bind(to stmt: SQLitePreparedStatement, parameter idx: Int32) throws {
		try serialized().bind(to: stmt, parameter: idx)
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
	public func column<T: DatabaseSerializable>(_ index: Int) throws -> T {
		return try T.deserialize(from: column(index))
	}

	/// Returns the value of column `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - returns: The column's value
	///
	/// - throws: An error if the column wasn't found or contains an illegal value
	public func column<T: DatabaseSerializable>(_ name: String) throws -> T {
		return try T.deserialize(from: column(name))
	}
}

extension Column {
	/// Returns the value of the column.
	///
	/// - returns: The column's value
	///
	/// - throws: An error if the column contains an illegal value
	public func value<T: DatabaseSerializable>() throws -> T {
		return try row.column(index)
	}
}

extension DatabaseSerializable where Self: NSCoding {
	public func serialized() -> DatabaseValue {
		return .blob(NSKeyedArchiver.archivedData(withRootObject: self))
	}

	public static func deserialize(from value: DatabaseValue) throws -> Self {
		switch value {
		case .blob(let b):
			guard let result = NSKeyedUnarchiver.unarchiveObject(with: b) as? Self else {
				throw DatabaseError.dataFormatError("\(value) is not a valid type")
			}
			return result
		default:
			throw DatabaseError.dataFormatError("\(value) is not a blob")
		}
	}
}

extension NSNumber: DatabaseSerializable {
	public func serialized() -> DatabaseValue {
		switch CFNumberGetType(self as CFNumber) {
		case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
		     .sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
			return DatabaseValue.integer(self.int64Value)

		case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
			return DatabaseValue.float(self.doubleValue)
		}
	}

	public static func deserialize(from value: DatabaseValue) throws -> Self {
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

extension NSNull: DatabaseSerializable {
	public func serialized() -> DatabaseValue {
		return .null
	}

	public static func deserialize(from value: DatabaseValue) throws -> Self {
		switch value {
		case .null:
			return self.init()
		default:
			throw DatabaseError.dataFormatError("\(value) is not null")
		}
	}
}
