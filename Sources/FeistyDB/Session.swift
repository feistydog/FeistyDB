//
//  Changeset.swift
//  FeistyDB
//
//  Created by Jason Jobe on 4/22/20.
//

import Foundation
#if DSQLITE_ENABLE_SESSION

/// An `sqlite3_snapshot *` object.
///
/// - seealso: [Database Snapshot](https://www.sqlite.org/c3ref/sessionintro.html)
public typealias SQLiteSession = UnsafeMutablePointer<sqlite3_session>

/// The state of a WAL mode database at a specific point in history.
public final class Changeset {
    /// The owning database
    public let database: Database

    /// The underlying `sqlite3_session *` object
    var session: SQLiteSession
    
    init(database: Database, schema: String) throws {
        self.database = database

        var session: SQLiteSession? = nil
        guard sqlite3session_create(database.db, schema, &session) == SQLITE_OK else {
            throw SQLiteError("Error creating database session", takingDescriptionFromDatabase: database.db)
        }

        self.session = session!
    }

    deinit {
        sqlite3session_delete(session)
    }
}

#endif

