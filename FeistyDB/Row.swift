//
// Copyright (c) 2015 - 2016 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

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
public final class Row {
	/// The statement owning this row.
	public let statement: Statement

	/// Creates a result row.
	///
	/// - parameter statement: The owning statement
	init(statement: Statement) {
		self.statement = statement
	}

	/// The number of columns in the row.
	public lazy var columnCount: Int = {
		return Int(sqlite3_data_count(self.statement.stmt))
	}()

	/// Returns the name of the column at `index`.
	///
	/// This is a shortcut for `statement.name(ofColumn: index)`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - throws: An error if `index` is out of bounds
	///
	/// - returns: The name of the column for the specified index
	public func name(ofColumn index: Int) throws -> String {
		return try statement.name(ofColumn: index)
	}

	/// Returns the index of the column `name`.
	///
	/// This is a shortcut for `index(ofColumn: name)`.
	///
	/// - parameter name: The name of the desired column
	///
	/// - throws: An error if the column doesn't exist
	///
	/// - returns: The index of the column with the specified name
	public func index(ofColumn name: String) throws -> Int {
		return try statement.index(ofColumn: name)
	}
}
