//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// An error supplying a message, SQLite error code, and description.
public struct SQLiteError: Error {
	/// A brief message describing the error
	public let message: String

	/// A result code specifying the error
	///
	/// - seealso: [Result and Error Codes](https://www.sqlite.org/rescode.html)
	public let code: SQLiteResult

	/// A more detailed description of the error's cause
	public let details: String?
}

extension SQLiteError {
	/// Creates an error with the given message and code.
	///
	/// The description is obtained using `sqlite3_errstr(code)`.
	///
	/// - parameter message: A brief message describing the error
	/// - parameter code: An SQLite error code
	public init(_ message: String, code: Int32) {
		self.message = message
		self.code = SQLiteResult(code)
		self.details = String(cString: sqlite3_errstr(code))
	}

	/// Creates an error with the given message, with result code and description obtained from `db`.
	///
	/// The error code is obtained using `sqlite3_extended_errcode(db)`.
	/// The description is obtained using `sqlite3_errmsg(db)`.
	///
	/// - parameter message: A brief message describing the error
	/// - parameter db: An `sqlite3 *` database connection handle
	public init(_ message: String, takingDescriptionFromDatabase db: SQLiteDatabaseConnection) {
		self.message = message
		self.code = SQLiteResult(sqlite3_extended_errcode(db))
		self.details = String(cString: sqlite3_errmsg(db))
	}

	/// Creates an error with the given message, with result code and description obtained from `stmt`.
	///
	/// The error code is obtained using `sqlite3_extended_errcode(sqlite3_db_handle(stmt))`.
	/// The description is obtained using `sqlite3_errmsg(sqlite3_db_handle(stmt))`.
	///
	/// - parameter message: A brief message describing the error
	/// - parameter stmt: An `sqlite3_stmt *` object
	public init(_ message: String, takingDescriptionFromStatement stmt: SQLitePreparedStatement) {
		self.message = message
		self.code = SQLiteResult(sqlite3_extended_errcode(sqlite3_db_handle(stmt)))
		self.details = String(cString: sqlite3_errmsg(sqlite3_db_handle(stmt)))
	}
}

extension SQLiteError: CustomStringConvertible {
	public var description: String {
		if let details = details {
			return "\(message) [\(code)]: \(details)"
		}
		else {
			return "\(message) [\(code)]"
		}
	}
}

extension SQLiteError: LocalizedError {
	public var errorDescription: String? {
		return message
	}

	public var failureReason: String? {
		return details
	}
}
