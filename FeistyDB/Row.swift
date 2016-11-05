/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A result row.
public struct Row {
	/// The statement owning this row.
	public let statement: Statement

	/// The number of columns in the row.
	public var columnCount: Int {
		return Int(sqlite3_column_count(statement.stmt))
	}

	/// Returns the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
	/// - precondition: `index >= 0`
	/// - precondition: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - returns: A column for the specified index
	///
	/// - throws: An error if `index` is out of bounds
	public func column(_ index: Int) throws -> Column {
		guard index >= 0, index < self.columnCount else {
			throw DatabaseError.sqliteError("Column index \(index) out of bounds")
		}
		return Column(row: self, index: index)
	}
}
