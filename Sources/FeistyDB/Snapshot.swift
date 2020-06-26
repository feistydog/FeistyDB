//
// Copyright (c) 2018 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// An `sqlite3_snapshot *` object.
///
/// - seealso: [Database Snapshot](https://www.sqlite.org/c3ref/snapshot.html)
public typealias SQLiteSnapshot = UnsafeMutablePointer<sqlite3_snapshot>

/// The state of a WAL mode database at a specific point in history.
public final class Snapshot {
	/// The owning database
	public let database: Database

	/// The underlying `sqlite3_snapshot *` object
	let snapshot: SQLiteSnapshot

	/// A snapshot of the current state of a database schema.
	///
	/// - note: If a read transaction is not already open one is opened automatically
	///
	/// - parameter database: The owning database
	/// - parameter schema: The database schema to snapshot
	///
	/// - throws: An error if the snapshot could not be recorded
	init(database: Database, schema: String) throws {
		self.database = database

		var snapshot: SQLiteSnapshot? = nil
		guard sqlite3_snapshot_get(database.db, schema, &snapshot) == SQLITE_OK else {
			throw SQLiteError("Error opening database snapshot", takingDescriptionFromDatabase: database.db)
		}

		self.snapshot = snapshot!
	}

	deinit {
		sqlite3_snapshot_free(snapshot)
	}
}

extension Snapshot: Comparable {
	public static func == (lhs: Snapshot, rhs: Snapshot) -> Bool {
//		precondition(lhs.database == rhs.database, "Cannot compare snapshots across databases")
		return sqlite3_snapshot_cmp(lhs.snapshot, rhs.snapshot) == 0
	}

	public static func < (lhs: Snapshot, rhs: Snapshot) -> Bool {
//		precondition(lhs.database == rhs.database, "Cannot compare snapshots across databases")
		return sqlite3_snapshot_cmp(lhs.snapshot, rhs.snapshot) < 0
	}
}

extension Database {
	/// Records a snapshot of the current state of a database schema.
	///
	/// - parameter schema: The database schema to snapshot
	///
	/// - throws: An error if the snapshot could not be recorded
	///
	/// - seealso: [Record A Database Snapshot](https://www.sqlite.org/c3ref/snapshot_get.html)
	public func takeSnapshot(schema: String = "main") throws -> Snapshot {
		return try Snapshot(database: self, schema: schema)
	}

	/// Starts or upgrades a read transaction for a database schema to a specific snapshot.
	///
	/// - parameter snapshot: The desired historical snapshot
	/// - parameter schema: The desired database schema
	///
	/// - throws: An error if the snapshot could not be opened
	///
	/// - seealso: [Start a read transaction on an historical snapshot](https://www.sqlite.org/c3ref/snapshot_open.html)
	public func openSnapshot(_ snapshot: Snapshot, schema: String = "main") throws {
		guard sqlite3_snapshot_open(db, schema, snapshot.snapshot) == SQLITE_OK else {
			throw SQLiteError("Error opening database snapshot", takingDescriptionFromDatabase: db)
		}
	}
}
