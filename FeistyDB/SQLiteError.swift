//
// Copyright (c) 2015 - 2018 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// A result code from SQLite
///
/// - seealso: [Result and Error Codes](http://www.sqlite.org/rescode.html)
public enum SQLiteResultCode {
	/// Successful result
	case ok(OKExtendedResultCode?)
	/// Generic error
	case error
	/// Internal logic error in SQLite
	case `internal`
	/// Access permission denied
	case perm
	/// Callback routine requested an abort
	case abort(AbortExtendedResultCode?)
	/// The database file is locked
	case busy(BusyExtendedResultCode?)
	/// A table in the database is locked
	case locked(LockedExtendedResultCode?)
	/// A malloc() failed
	case noMem
	/// Attempt to write a readonly database
	case readOnly(ReadOnlyExtendedResultCode?)
	/// Operation terminated by sqlite3_interrupt()
	case interrupt
	/// Some kind of disk I/O error occurred
	case ioErr(IOErrExtendedResultCode?)
	/// The database disk image is malformed
	case corrupt(CorruptExtendedResultCode?)
	/// Unknown opcode in sqlite3_file_control()
	case notFound
	/// Insertion failed because database is full
	case full
	/// Unable to open the database file
	case cantOpen(CantOpenExtendedResultCode?)
	/// Database lock protocol error
	case `protocol`
	/// Internal use only
	case empty
	/// The database schema changed
	case schema
	/// String or BLOB exceeds size limit
	case tooBig
	/// Abort due to constraint violation
	case constraint(ConstraintExtendedResultCode?)
	/// Data type mismatch
	case mismatch
	/// Library used incorrectly
	case misuse
	/// Uses OS features not supported on host
	case noLFS
	/// Authorization denied
	case auth(AuthExtendedResultCode?)
	/// Not used
	case format
	/// 2nd parameter to sqlite3_bind out of range
	case range
	/// File opened that is not a database file
	case notADB
	/// Notifications from sqlite3_log()
	case notice(NoticeExtendedResultCode?)
	/// Warnings from sqlite3_log()
	case warning(WarningExtendedResultCode?)
	/// sqlite3_step() has another row ready
	case row
	/// sqlite3_step() has finished executing
	case done

	// Extended result codes

	/// SQLITE_IOERR_* extended result codes
	public enum IOErrExtendedResultCode {
		/// SQLITE_IOERR_READ
		case read
		/// SQLITE_IOERR_SHORT_READ
		case shortRead
		/// SQLITE_IOERR_WRITE
		case write
		/// SQLITE_IOERR_FSYNC
		case fSync
		/// SQLITE_IOERR_DIR_FSYNC
		case dirFSync
		/// SQLITE_IOERR_TRUNCATE
		case truncate
		/// SQLITE_IOERR_FSTAT
		case fStat
		/// SQLITE_IOERR_UNLOCK
		case unlock
		/// SQLITE_IOERR_RDLOCK
		case rdLock
		/// SQLITE_IOERR_DELETE
		case delete
		/// SQLITE_IOERR_BLOCKED
		case blocked
		/// SQLITE_IOERR_NOMEM
		case noMem
		/// SQLITE_IOERR_ACCESS
		case access
		/// SQLITE_IOERR_CHECKRESERVEDLOCK
		case checkReservedLock
		/// SQLITE_IOERR_LOCK
		case lock
		/// SQLITE_IOERR_CLOSE
		case close
		/// SQLITE_IOERR_DIR_CLOSE
		case dirClose
		/// SQLITE_IOERR_SHMOPEN
		case shmOpen
		/// SQLITE_IOERR_SHMSIZE
		case shmSize
		/// SQLITE_IOERR_SHMLOCK
		case shmLock
		/// SQLITE_IOERR_SHMMAP
		case shmMap
		/// SQLITE_IOERR_SEEK
		case seek
		/// SQLITE_IOERR_DELETE_NOENT
		case deleteNoEnt
		/// SQLITE_IOERR_MMAP
		case mmap
		/// SQLITE_IOERR_GETTEMPPATH
		case getTempPath
		/// SQLITE_IOERR_CONVPATH
		case convPath
		/// SQLITE_IOERR_VNODE
		case vnode
		/// SQLITE_IOERR_AUTH
		case auth
		/// SQLITE_IOERR_BEGIN_ATOMIC
		case beginAtomic
		/// SQLITE_IOERR_COMMIT_ATOMIC
		case commitAtomic
		/// SQLITE_IOERR_ROLLBACK_ATOMIC
		case rollbackAtomic
	}

	/// SQLITE_LOCKED_* extended result codes
	public enum LockedExtendedResultCode {
		/// SQLITE_LOCKED_SHAREDCACHE
		case sharedCache
	}

	/// SQLITE_BUSY_* extended result codes
	public enum BusyExtendedResultCode {
		/// SQLITE_BUSY_RECOVERY
		case recovery
		/// SQLITE_BUSY_SNAPSHOT
		case snapshot
	}

	/// SQLITE_CANTOPEN_* extended result codes
	public enum CantOpenExtendedResultCode {
		/// SQLITE_CANTOPEN_NOTEMPDIR
		case noTempDir
		/// SQLITE_CANTOPEN_ISDIR
		case isDir
		/// SQLITE_CANTOPEN_FULLPATH
		case fullPath
		/// SQLITE_CANTOPEN_CONVPATH
		case convPath
	}

	/// SQLITE_CORRUPT_* extended result codes
	public enum CorruptExtendedResultCode {
		/// SQLITE_CORRUPT_VTAB
		case vtab
	}

	/// SQLITE_READONLY_* extended result codes
	public enum ReadOnlyExtendedResultCode {
		/// SQLITE_READONLY_RECOVERY
		case recovery
		/// SQLITE_READONLY_CANTLOCK
		case cantLock
		/// SQLITE_READONLY_ROLLBACK
		case rollback
		/// SQLITE_READONLY_DBMOVED
		case dbMoved
	}

	/// SQLITE_ABORT_* extended result codes
	public enum AbortExtendedResultCode {
		/// SQLITE_ABORT_ROLLBACK
		case rollback
	}

	/// SQLITE_CONSTRAINT_* extended result codes
	public enum ConstraintExtendedResultCode {
		/// SQLITE_CONSTRAINT_CHECK
		case check
		/// SQLITE_CONSTRAINT_COMMITHOOK
		case commitHook
		/// SQLITE_CONSTRAINT_FOREIGNKEY
		case foreignKey
		/// SQLITE_CONSTRAINT_FUNCTION
		case function
		/// SQLITE_CONSTRAINT_NOTNULL
		case notNull
		/// SQLITE_CONSTRAINT_PRIMARYKEY
		case primaryKey
		/// SQLITE_CONSTRAINT_TRIGGER
		case trigger
		/// SQLITE_CONSTRAINT_UNIQUE
		case unique
		/// SQLITE_CONSTRAINT_VTAB
		case vtab
		/// SQLITE_CONSTRAINT_ROWID
		case rowid
	}

	/// SQLITE_NOTICE_* extended result codes
	public enum NoticeExtendedResultCode {
		/// SQLITE_NOTICE_RECOVER_WAL
		case recoverWAL
		/// SQLITE_NOTICE_RECOVER_ROLLBACK
		case recoverRollback
	}

	/// SQLITE_WARNING_* extended result codes
	public enum WarningExtendedResultCode {
		/// SQLITE_WARNING_AUTOINDEX
		case autoindex
	}

	/// SQLITE_AUTH_* extended result codes
	public enum AuthExtendedResultCode {
		/// SQLITE_AUTH_USER
		case user
	}

	/// SQLITE_OK_* extended result codes
	public enum OKExtendedResultCode {
		/// SQLITE_OK_LOAD_PERMANENTLY
		case loadPermanently
	}
}

extension SQLiteResultCode {
	init(_ code: Int32) {
		let primary = code & 0xff
		let extended = code >> 8
		switch primary {
		case SQLITE_OK:
			switch(extended) {
			case 1:					self = .ok(.loadPermanently)
			default:				self = .ok(nil)
			}
		case SQLITE_ERROR: 			self = .error
		case SQLITE_INTERNAL:		self = .`internal`
		case SQLITE_PERM:			self = .perm
		case SQLITE_ABORT:
			switch(extended) {
			case 2: 				self = .abort(.rollback)
			default:				self = .abort(nil)
			}
		case SQLITE_BUSY:
			switch(extended) {
			case 1: 				self = .busy(.recovery)
			case 2: 				self = .busy(.snapshot)
			default:				self = .busy(nil)
			}
		case SQLITE_LOCKED:
			switch(extended) {
			case 1: 				self = .locked(.sharedCache)
			default:				self = .locked(nil)
			}
		case SQLITE_NOMEM:			self = .noMem
		case SQLITE_READONLY:
			switch(extended) {
			case 1: 				self = .readOnly(.recovery)
			case 2: 				self = .readOnly(.cantLock)
			case 3: 				self = .readOnly(.rollback)
			case 4: 				self = .readOnly(.dbMoved)
			default:				self = .readOnly(nil)
			}
		case SQLITE_INTERRUPT:		self = .interrupt
		case SQLITE_IOERR:
			switch(extended) {
			case 1: 				self = .ioErr(.read)
			case 2: 				self = .ioErr(.shortRead)
			case 3: 				self = .ioErr(.write)
			case 4: 				self = .ioErr(.fSync)
			case 5: 				self = .ioErr(.dirFSync)
			case 6: 				self = .ioErr(.truncate)
			case 7: 				self = .ioErr(.fStat)
			case 8: 				self = .ioErr(.unlock)
			case 9: 				self = .ioErr(.rdLock)
			case 10: 				self = .ioErr(.delete)
			case 11: 				self = .ioErr(.blocked)
			case 12: 				self = .ioErr(.noMem)
			case 13: 				self = .ioErr(.access)
			case 14: 				self = .ioErr(.checkReservedLock)
			case 15: 				self = .ioErr(.lock)
			case 16: 				self = .ioErr(.close)
			case 17: 				self = .ioErr(.dirClose)
			case 18: 				self = .ioErr(.shmOpen)
			case 19: 				self = .ioErr(.shmSize)
			case 20: 				self = .ioErr(.shmLock)
			case 21: 				self = .ioErr(.shmMap)
			case 22: 				self = .ioErr(.seek)
			case 23: 				self = .ioErr(.deleteNoEnt)
			case 24: 				self = .ioErr(.mmap)
			case 25: 				self = .ioErr(.getTempPath)
			case 26: 				self = .ioErr(.convPath)
			case 27: 				self = .ioErr(.vnode)
			case 28: 				self = .ioErr(.auth)
			case 29: 				self = .ioErr(.beginAtomic)
			case 30: 				self = .ioErr(.commitAtomic)
			case 31: 				self = .ioErr(.rollbackAtomic)
			default:				self = .ioErr(nil)
			}
		case SQLITE_CORRUPT:
			switch(extended) {
			case 1: 				self = .corrupt(.vtab)
			default:				self = .corrupt(nil)
			}
		case SQLITE_NOTFOUND:		self = .notFound
		case SQLITE_FULL:			self = .full
		case SQLITE_CANTOPEN:
			switch(extended) {
			case 1: 				self = .cantOpen(.noTempDir)
			case 2: 				self = .cantOpen(.isDir)
			case 3: 				self = .cantOpen(.fullPath)
			case 4: 				self = .cantOpen(.convPath)
			default:				self = .cantOpen(nil)
			}
		case SQLITE_PROTOCOL:		self = .`protocol`
		case SQLITE_EMPTY:			self = .empty
		case SQLITE_SCHEMA:			self = .schema
		case SQLITE_TOOBIG:			self = .tooBig
		case SQLITE_CONSTRAINT:
			switch(extended) {
			case 1:					self = .constraint(.check)
			case 2:					self = .constraint(.commitHook)
			case 3:					self = .constraint(.foreignKey)
			case 4:					self = .constraint(.function)
			case 5:					self = .constraint(.notNull)
			case 6:					self = .constraint(.primaryKey)
			case 7:					self = .constraint(.trigger)
			case 8:					self = .constraint(.unique)
			case 9:					self = .constraint(.vtab)
			case 10:				self = .constraint(.rowid)
			default:				self = .constraint(nil)
			}
		case SQLITE_MISMATCH:		self = .mismatch
		case SQLITE_MISUSE:			self = .misuse
		case SQLITE_NOLFS:			self = .noLFS
		case SQLITE_AUTH:
			switch(extended) {
			case 1:					self = .auth(.user)
			default:				self = .auth(nil)
			}
		case SQLITE_FORMAT:			self = .format
		case SQLITE_RANGE:			self = .range
		case SQLITE_NOTADB:			self = .notADB
		case SQLITE_NOTICE:
			switch(extended) {
			case 1:					self = .notice(.recoverWAL)
			case 2:					self = .notice(.recoverRollback)
			default:				self = .notice(nil)
			}
		case SQLITE_WARNING:
			switch(extended) {
			case 1:					self = .warning(.autoindex)
			default:				self = .warning(nil)
			}
		case SQLITE_ROW:			self = .row
		case SQLITE_DONE:			self = .done

		default:
			preconditionFailure("Unknown SQLite result code")
		}
	}
}

/// An error supplying a message, SQLite error code, and description.
public struct SQLiteError: Error {
	/// A brief message describing the error
	public let message: String

	/// A result code specifying the error
	///
	/// - seealso: [Result and Error Codes](http://www.sqlite.org/rescode.html)
	public let code: SQLiteResultCode

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
		self.code = SQLiteResultCode(code)
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
		self.code = SQLiteResultCode(sqlite3_extended_errcode(db))
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
		self.code = SQLiteResultCode(sqlite3_extended_errcode(sqlite3_db_handle(stmt)))
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
