/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A result row.
///
/// A `Row` is not created directly but is obtained from a `Statement`, either via `execute()` or iteration:
///
/// ```swift
/// try statement.execute() { row in
///     // Do something with `row`
/// }
/// ```
///
/// ```swift
/// for row in statement {
///     // Do something with `row`
/// }
/// ```
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
