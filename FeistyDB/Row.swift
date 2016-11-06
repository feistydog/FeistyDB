/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A result row containing one or more columns with type-safe value access.
///
/// **Creation**
///
/// A row is not created directly but is obtained from a `Statement`.
///
/// ```swift
/// try statement.execute() { row in
///     // Do something with `row`
/// }
/// ```
///
/// **Column Value Access**
///
/// The database-native column value is expressed by `DatabaseValue`, however custom type conversion is possible when
/// a type implements either the `ColumnConvertible` or `DatabaseSerializable` protocol.
///
/// The value of columns is accessed by the `value(at:)` or `value(named:)` methods.
///
/// ```swift
/// let value = try row.value(at: 0)
/// let uuid: UUID = try row.value(named: "session_uuid")
/// ```
///
/// It is also possible to iterate over column values:
///
/// ```swift
/// for row in statement {
///     for value in row {
///         // Do something with `value`
///     }
///
/// }
/// ```
///
/// This allows for simple result row processing at the expense of error handling.
public struct Row {
	/// The statement owning this row.
	public let statement: Statement
}

extension Row {
	/// The number of columns in the row.
	public var columnCount: Int {
		return statement.columnCount
	}

	/// Returns the name of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - returns: The name of the column for the specified index
	/// - throws: An error if `index` is out of bounds
	public func name(ofColumn index: Int) throws -> String {
		return try statement.name(ofColumn: index)
	}
}
