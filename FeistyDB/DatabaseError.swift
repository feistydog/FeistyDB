//
// Copyright (c) 2015 - 2017 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// An error supplying a message and description.
public struct DatabaseError: Error {
	/// A brief message describing the error
	public let message: String

	/// A more detailed description of the error's cause
	public let details: String?
}

extension DatabaseError {
	/// Creates an error with the given message.
	///
	/// - parameter message: A brief message describing the error
	public init(_ message: String) {
		self.message = message
		self.details = nil
	}

	/// Creates an error with the given message and description obtained from `db`.
	///
	/// The description is obtained using `sqlite3_errmsg(db)`.
	///
	/// - parameter message: A brief message describing the error
	/// - parameter db: An `sqlite3 *` database connection handle
	public init(message: String, takingDescriptionFromDatabase db: SQLiteDatabaseConnection) {
		self.message = message
		self.details = String(cString: sqlite3_errmsg(db))
	}

	/// Creates an error with the given message and description obtained from `stmt`.
	///
	/// The description is obtained using `sqlite3_errmsg(sqlite3_db_handle(stmt))`.
	///
	/// - parameter message: A brief message describing the error
	/// - parameter stmt: An `sqlite3_stmt *` object
	public init(message: String, takingDescriptionFromStatement stmt: SQLitePreparedStatement) {
		self.message = message
		self.details = String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt)))
	}
}

extension DatabaseError: CustomStringConvertible {
	public var description: String {
		if let details = details {
			return "\(message): \(details)"
		}
		else {
			return message
		}
	}
}
