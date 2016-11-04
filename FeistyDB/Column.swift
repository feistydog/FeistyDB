/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A single column in a result row.
public struct Column {
	/// The result row containing this column.
	public let row: Row

	/// The index of the column in `self.row`.
	public let index: Int

	/// The name of the result row column.
	///
	/// The name is the value of the `AS` clause
	public var name: String {
		return String(cString: sqlite3_column_name(row.statement.stmt, Int32(index)))
	}

	// The following functions are omitted for performance reasons.
	// SQLITE_OMIT_DECLTYPE is a recommended compile time option, and it
	// is incompatible with SQLITE_ENABLE_COLUMN_METADATA which these
	// functions require.
	#if false
		/// The un-aliased name of the database that is the origin of this result row column
		public var databaseName: String {
			return String(cString: sqlite3_column_database_name(row.statement.stmt, Int32(index)))
		}

		/// The un-aliased name of the table that is the origin of this result row column
		public var tableName: String {
			return String(cString: sqlite3_column_table_name(row.statement.stmt, Int32(index)))
		}

		/// The un-aliased name of the column that is the origin of this result row column
		public var originName: String {
			return String(cString: sqlite3_column_origin_name(row.statement.stmt, Int32(index)))
		}
	#endif
}
