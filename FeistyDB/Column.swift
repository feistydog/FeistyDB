/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// A struct representing a single column in a row
public struct Column {
	/// The owning `Row`
	public let row: Row

	/// The column index
	public let index: Int

	/// The name of the result set column
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
		/// The un-aliased name of the database that is the origin of this result column
		public var databaseName: String {
			return String(cString: sqlite3_column_database_name(row.statement.stmt, Int32(index)))
		}

		/// The un-aliased name of the table that is the origin of this result column
		public var tableName: String {
			return String(cString: sqlite3_column_table_name(row.statement.stmt, Int32(index)))
		}

		/// The un-aliased name of the column that is the origin of this result column
		public var originName: String {
			return String(cString: sqlite3_column_origin_name(row.statement.stmt, Int32(index)))
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
	public func withUnsafeRawSQLiteStatement<T>(block: (_ statement: SQLitePreparedStatement, _ index: Int) throws -> (T)) rethrows -> T {
		return try block(row.statement.stmt, index)
	}
}
