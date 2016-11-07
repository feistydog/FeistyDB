/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// An error supplying a message and description.
public struct DatabaseError: Error {
	/// A brief message describing the error
	public let message: String

	/// A more detailed description of the error's cause
	public let description: String?
}

extension DatabaseError {
	/// Creates an error with the given message and description.
	///
	/// - parameter message: A brief message describing the error
	/// - parameter description: A more detailed description of the error's cause
	public init(_ message: String) {
		self.message = message
		self.description = nil
	}

	/// Creates an error with the given message and description obtained from `db`.
	///
	/// The description is obtained using `sqlite3_errmsg(db)`.
	///
	/// - parameter message: A brief message describing the error
	/// - parameter db: An `sqlite3 *` database connection handle
	public init(message: String, takingDescriptionFromDatabase db: SQLiteDatabaseConnection) {
		self.message = message
		self.description = String(cString: sqlite3_errmsg(db))
	}

	/// Creates an error with the given message and description obtained from `stmt`.
	///
	/// The description is obtained using `sqlite3_errmsg(sqlite3_db_handle(stmt))`.
	///
	/// - parameter message: A brief message describing the error
	/// - parameter stmt: An `sqlite3_stmt *` object
	public init(message: String, takingDescriptionFromStatement stmt: SQLitePreparedStatement) {
		self.message = message
		self.description = String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt)))
	}
}
