/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A class representing a single row in a result set
public struct Row {
	/// The underlying `sqlite3_stmt *`
	var stmt: OpaquePointer

	/// Initialize a new row with a compiled SQL statement
	///
	/// - parameter statement: An `sqlite3_stmt *` object
	init(_ statement: OpaquePointer) {
		stmt = statement
	}

	/// The number of columns in the row
	public var columnCount: Int {
		return Int(sqlite3_column_count(stmt))
	}

	/// Retrieve a column from the row
	///
	/// - parameter index: The 0-based index of the desired column
	/// - returns: A column for the specified index
	public func column(_ index: Int) -> Column {
//		precondition(index >= 0, "Column indexes are 0-based")
//		precondition(index < self.columnCount, "Column index out of bounds")
		return Column(row: self, index: index)
	}
}
