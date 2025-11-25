//
// Copyright (c) 2021 - 2025 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import CSQLite

/// FeistyDB global functions.
public struct FeistyDB {
	/// Initializes SQLite and registers the SQLite extensions used by FeistyDB
	///
	/// - note: This *must* be called once before any other functions in `FeistyDB`
	///
	/// - throws: An error if SQLite initialization or extension registration fails
	public static func initialize() throws {
		var rc = sqlite3_initialize()
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error initializing sqlite3", code: rc)
		}
		rc = csqlite_sqlite3_auto_extension_decimal()
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error initializing decimal extension", code: rc)
		}
		rc = csqlite_sqlite3_auto_extension_ieee754()
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error initializing ieee754 extension", code: rc)
		}
		rc = csqlite_sqlite3_auto_extension_series()
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error initializing series extension", code: rc)
		}
		rc = csqlite_sqlite3_auto_extension_sha3()
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error initializing sha3 extension", code: rc)
		}
		rc = csqlite_sqlite3_auto_extension_uuid()
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error initializing uuid extension", code: rc)
		}
	}
}

