//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation
import CSQLite

/// A type that may be serialized to and deserialized from a database.
///
/// This is a more general method for database storage than `ParameterBindable` and `ColumnConvertible`
/// because it allows types to customize their behavior based on the database value's data type. 
/// A database value's data type is the value returned by the `sqlite3_column_type()` before any
/// type conversions have taken place.
///
/// - note: Columns in SQLite have a type affinity (declared type) while stored values have an 
/// individual storage class/data type.  There are rules for conversion which are documented
/// at [Datatypes In SQLite Version 3](https://sqlite.org/datatype3.html).
///
/// For example, `NSNumber` can choose what to store in the database based on the boxed value:
///
/// ```swift
/// extension NSNumber: DatabaseSerializable {
///     public func serialized() -> DatabaseValue {
///         switch CFNumberGetType(self as CFNumber) {
///         case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
///              .sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
///             return DatabaseValue.integer(self.int64Value)
///
///         case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
///             return DatabaseValue.float(self.doubleValue)
///         }
///     }
///
///     public static func deserialize(from value: DatabaseValue) throws -> Self {
///         switch value {
///         case .integer(let i):
///             return self.init(value: i)
///         case .float(let f):
///             return self.init(value: f)
///         default:
///             throw DatabaseError("\(value) is not a number")
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
	///
	/// - throws: An error if `value` contains an illegal value for `Self`
	///
	/// - returns: An instance of `Self`
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
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - throws: An error if the column contains an illegal value
	///
	/// - returns: The column's value
	public func value<T: DatabaseSerializable>(at index: Int) throws -> T {
		return try T.deserialize(from: value(at: index))
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - throws: An error if the column wasn't found or contains an illegal value
	///
	/// - returns: The column's value
	public func value<T: DatabaseSerializable>(named name: String) throws -> T {
		return try T.deserialize(from: value(named: name))
	}
}

extension Statement {
	/// Returns the values of the columns at `indexes` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `indexes.min() >= 0`
	/// - requires: `indexes.max() < self.columnCount`
	///
	/// - parameter indexes: The indexes of the desired columns
	///
	/// - throws: An error if any element of `indexes` is out of bounds
	public func columns<S: Collection, T: DatabaseSerializable>(_ indexes: S) throws -> [[T]] where S.Element == Int {
		var values = [[T]](repeating: [], count: indexes.count)
		try results { row in
			for (n, x) in indexes.enumerated() {
				values[n].append(try row.value(at: x))
			}
		}
		return values
	}

	/// Returns the value of the column at `index` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - throws: An error if `index` is out of bounds or the column contains a null or illegal value
	public func column<T: DatabaseSerializable>(_ index: Int) throws -> [T] {
		var values = [T]()
		try results { row in
			values.append(try row.value(at: index))
		}
		return values
	}

	/// Returns the value of the leftmost column for each row in the result set.
	///
	/// This is a shortcut for `column(0)`.
	///
	/// - throws: An error if there are no columns
	///
	/// - returns: An array containing the leftmost column's values
	public func leftmostColumn<T: DatabaseSerializable>() throws -> [T] {
		return try column(0)
	}
}

extension Row {
	/// Returns the value of the leftmost column.
	///
	/// This is a shortcut for `value(at: 0)`.
	///
	/// - throws: An error if there are no columns or the column contains an illegal value
	///
	/// - returns: The first column's value or `nil` if null
	public func leftmostValue<T: DatabaseSerializable>() throws -> T {
		return try value(at: 0)
	}
}

extension Statement {
	/// Returns the value of the leftmost column in the first row.
	///
	/// - throws: An error if there are no columns or the column contains an illegal value
	///
	/// - returns: The value of the leftmost column in the first row
	public func front<T: DatabaseSerializable>() throws -> T? {
		guard let row = try firstRow() else {
			return nil
		}
		return try row.value(at: 0) as T
	}

	/// Returns the value of the leftmost column in the first row.
	///
	/// - throws: An error if there are no rows, no columns, or the column contains an illegal value
	///
	/// - returns: The value of the leftmost column in the first row
	public func front<T: DatabaseSerializable>() throws -> T {
		guard let row = try firstRow() else {
			throw DatabaseError("Statement returned no rows")
		}
		return try row.value(at: 0)
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
				throw DatabaseError("\(value) is not a valid instance of \(Self.self)")
			}
			return result
		default:
			throw DatabaseError("\(value) is not a blob")
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
		@unknown default:
			return DatabaseValue.null
		}
	}

	public static func deserialize(from value: DatabaseValue) throws -> Self {
		switch value {
		case .integer(let i):
			return self.init(value: i)
		case .float(let f):
			return self.init(value: f)
		default:
			throw DatabaseError("\(value) is not a number")
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
			throw DatabaseError("\(value) is not null")
		}
	}
}
