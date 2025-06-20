//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import os.log
import Foundation
import CSQLite

/// A native data type that may be stored in an SQLite database.
///
/// - seealso: [Datatypes In SQLite Version 3](https://sqlite.org/datatype3.html)
public enum DatabaseValue {
	/// An integer value.
	case integer(Int64)
	/// A floating-point value.
	case float(Double)
	/// A text value.
	case text(String)
	/// A blob (untyped bytes) value.
	case blob(Data)
	/// A null value.
	case null
}

extension DatabaseValue: Equatable {
	public static func ==(lhs: DatabaseValue, rhs: DatabaseValue) -> Bool {
		switch(lhs) {
		case .integer(let i1):
			switch(rhs) {
			case .integer(let i2):
				return i1 == i2
			default:
				return false
			}
		case .float(let f1):
			switch(rhs) {
			case .float(let f2):
				return f1 == f2
			default:
				return false
			}
		case .text(let t1):
			switch(rhs) {
			case .text(let t2):
				return t1 == t2
			default:
				return false
			}
		case .blob(let b1):
			switch(rhs) {
			case .blob(let b2):
				return b1 == b2
			default:
				return false
			}
		case .null:
			return false
		}
	}
}

extension DatabaseValue: ExpressibleByNilLiteral {
	public init(nilLiteral: ()) {
		self = .null
	}
}

extension DatabaseValue: ExpressibleByIntegerLiteral {
	public init(integerLiteral value: IntegerLiteralType) {
		self = .integer(Int64(value))
	}
}

extension DatabaseValue: ExpressibleByFloatLiteral {
	public init(floatLiteral value: FloatLiteralType) {
		self = .float(value)
	}
}

extension DatabaseValue: ExpressibleByStringLiteral {
	public init(stringLiteral value: StringLiteralType) {
		self = .text(value)
	}
}

extension DatabaseValue: ExpressibleByBooleanLiteral {
	public init(booleanLiteral value: BooleanLiteralType) {
		self = .integer(value ? 1 : 0)
	}
}

extension DatabaseValue {
	/// Creates an instance containing the value of column `idx` in `stmt`.
	///
	/// - note: No bounds checking is performed on `idx`
	///
	/// - parameter stmt: An `sqlite3_stmt *` object
	/// - parameter idx: The index of the desired column
	init(_ stmt: SQLitePreparedStatement, column idx: Int32) {
		let type = sqlite3_column_type(stmt, idx)
		switch type {
		case SQLITE_INTEGER:
			self = .integer(sqlite3_column_int64(stmt, idx))

		case SQLITE_FLOAT:
			self = .float(sqlite3_column_double(stmt, idx))

		case SQLITE_TEXT:
			self = .text(String(cString: sqlite3_column_text(stmt, idx)))

		case SQLITE_BLOB:
			let byteCount = Int(sqlite3_column_bytes(stmt, idx))
			let data = Data(bytes: sqlite3_column_blob(stmt, idx).assumingMemoryBound(to: UInt8.self), count: byteCount)
			self = .blob(data)

		case SQLITE_NULL:
			self = .null

		default:
			os_log("Unknown SQLite column type %d encountered", type: .info, type);
			self = .null
		}
	}
}

/// An `sqlite3_value *` object.
///
/// - seealso: [Obtaining SQL Values](https://sqlite.org/c3ref/value_blob.html)
typealias SQLiteValue = OpaquePointer

extension DatabaseValue {
	/// Creates an instance containing `value`.
	///
	/// - parameter value: An `sqlite3_value *` object
	init(_ value: SQLiteValue) {
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
			os_log("Unknown SQLite value type %d encountered", type: .info, type);
			self = .null
		}
	}
}

extension DatabaseValue: CustomStringConvertible {
	/// A description of the type and value of `self`.
	public var description: String {
		switch self {
		case .integer(let i):
			return ".integer(\(i))"
		case .float(let f):
			return ".float(\(f))"
		case .text(let t):
			return ".text(\"\(t)\")"
		case .blob(let b):
			return ".blob(\(b))"
		case .null:
			return ".null"
		}
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
	/// - throws: An error if `index` is out of bounds
	///
	/// - returns: The column's value
	public func value(at index: Int) throws -> DatabaseValue {
		guard index >= 0, index < self.columnCount else {
			throw DatabaseError("Column index \(index) out of bounds")
		}
		return DatabaseValue(statement.stmt, column: Int32(index))
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - throws: An error if the column doesn't exist
	///
	/// - returns: The column's value
	public func value(named name: String) throws -> DatabaseValue {
		return try value(at: statement.index(ofColumn: name))
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
	public func columns<S: Collection>(_ indexes: S) throws -> [[DatabaseValue]] where S.Element == Int {
		var values = [[DatabaseValue]](repeating: [], count: indexes.count)
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
	/// - throws: An error if `index` is out of bounds
	public func column(_ index: Int) throws -> [DatabaseValue] {
		var values = [DatabaseValue]()
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
	public func leftmostColumn() throws -> [DatabaseValue] {
		return try column(0)
	}
}

extension Row {
	/// Returns the value of the leftmost column.
	///
	/// This is a shortcut for `value(at: 0)`.
	///
	/// - throws: An error if there are no columns
	///
	/// - returns: The column's value
	public func leftmostValue() throws -> DatabaseValue {
		return try value(at: 0)
	}
}

extension Row {
	/// Returns a dictionary of the row's values keyed by column name.
	///
	/// - returns: A dictionary of the row's values
	public func values() -> [String: DatabaseValue] {
		var values = [String: DatabaseValue](minimumCapacity: Int(columnCount))
		let stmt = statement.stmt
		for (name, index) in statement.columnNamesAndIndexes {
			values[name] = DatabaseValue(stmt, column: Int32(index))
		}
		return values
	}
}

extension Row: CustomStringConvertible {
	/// A description of the type and value of `self`.
	public var description: String {
		return values().description
	}
}

extension Statement {
	/// Returns the value of the leftmost column in the first result row.
	///
	/// - throws: An error if there are no columns
	///
	/// - returns: The value of the leftmost column in the first result row
	public func front() throws -> DatabaseValue? {
		return try firstRow()?.value(at: 0)
	}

	/// Returns the value of the leftmost column in the first result row.
	///
	/// - throws: An error if there are no rows or columns
	///
	/// - returns: The value of the leftmost column in the first result row
	public func front() throws -> DatabaseValue {
		guard let row = try firstRow() else {
			throw DatabaseError("Statement returned no rows")
		}
		return try row.value(at: 0)
	}
}

extension Row {
	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - returns: The column's value or `nil` if the column doesn't exist
	public subscript(name: String) -> DatabaseValue? {
		return try? value(named: name)
	}
}

extension Row: Collection {
	public var startIndex: Int {
		return 0
	}

	public var endIndex: Int {
		return columnCount
	}

	public subscript(position: Int) -> DatabaseValue {
		do {
			return try value(at: position)
		}
		catch {
			return .null
		}
	}

	public func index(after i: Int) -> Int {
		return i + 1
	}
}
