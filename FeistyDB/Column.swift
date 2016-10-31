/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A struct representing a single column in a row
public struct Column {
	/// The owning `Row`
	var row: Row
	/// The column index
	var idx: Int32

	/// Initialize a new column with a compiled SQL statement and column index
	///
	/// - parameter row: The owning `Row` object
	/// - parameter index: The desired column index
	/// - precondition: `index` >= 0
	init(row: Row, index: Int) {
//		precondition(index >= 0, "Column indexes are 0-based")
		self.row = row
		idx = Int32(index)
	}

	/// The name of the result set column
	///
	/// The name is the value of the `AS` clause
	public var name: String {
		return String(cString: sqlite3_column_name(row.statement.stmt, idx))
	}

	// The following functions are omitted for performance reasons.
	// SQLITE_OMIT_DECLTYPE is a recommended compile time option, and it
	// is incompatible with SQLITE_ENABLE_COLUMN_METADATA which these
	// functions require.
	#if false
		/// The un-aliased name of the database that is the origin of this result column
		public var databaseName: String {
			return String(cString: sqlite3_column_database_name(row.statement.stmt, idx))
		}

		/// The un-aliased name of the table that is the origin of this result column
		public var tableName: String {
			return String(cString: sqlite3_column_table_name(row.statement.stmt, idx))
		}

		/// The un-aliased name of the column that is the origin of this result column
		public var originName: String {
			return String(cString: sqlite3_column_origin_name(row.statement.stmt, idx))
		}
	#endif

	/// Perform a low-level statement operation
	///
	/// **Use of this function should be avoided whenever possible**
	///
	/// - parameter block: The block performing the operation
	/// - parameter statement: The raw `sqlite3_stmt *` statement object
	/// - parameter index: The index of this column in the result set
	/// - throws: Any error thrown in `block`
	/// - returns: The value returned by `block`
	public func withUnsafeRawSQLiteStatement<T>(block: (_ statement: OpaquePointer, _ index: Int32) throws -> (T)) rethrows -> T {
		return try block(row.statement.stmt, idx)
	}
}
