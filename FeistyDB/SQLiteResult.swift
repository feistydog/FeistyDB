//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// A result code from SQLite
///
/// - seealso: [Result and Error Codes](https://www.sqlite.org/rescode.html)
public enum SQLiteResult {
	/// Successful result
	case ok(OKExtendedResultCode? = nil)
	/// Generic error
	case error(ErrorExtendedResultCode? = nil)
	/// Internal logic error in SQLite
	case `internal`
	/// Access permission denied
	case perm
	/// Callback routine requested an abort
	case abort(AbortExtendedResultCode? = nil)
	/// The database file is locked
	case busy(BusyExtendedResultCode? = nil)
	/// A table in the database is locked
	case locked(LockedExtendedResultCode? = nil)
	/// A malloc() failed
	case noMem
	/// Attempt to write a readonly database
	case readOnly(ReadOnlyExtendedResultCode? = nil)
	/// Operation terminated by `sqlite3_interrupt()`
	case interrupt
	/// Some kind of disk I/O error occurred
	case ioErr(IOErrExtendedResultCode? = nil)
	/// The database disk image is malformed
	case corrupt(CorruptExtendedResultCode? = nil)
	/// Unknown opcode in `sqlite3_file_control()`
	case notFound
	/// Insertion failed because database is full
	case full
	/// Unable to open the database file
	case cantOpen(CantOpenExtendedResultCode? = nil)
	/// Database lock protocol error
	case `protocol`
	/// Internal use only
	case empty
	/// The database schema changed
	case schema
	/// String or BLOB exceeds size limit
	case tooBig
	/// Abort due to constraint violation
	case constraint(ConstraintExtendedResultCode? = nil)
	/// Data type mismatch
	case mismatch
	/// Library used incorrectly
	case misuse
	/// Uses OS features not supported on host
	case noLFS
	/// Authorization denied
	case auth(AuthExtendedResultCode? = nil)
	/// Not used
	case format
	/// 2nd parameter to `sqlite3_bind()` out of range
	case range
	/// File opened that is not a database file
	case notADB
	/// Notifications from `sqlite3_log()`
	case notice(NoticeExtendedResultCode? = nil)
	/// Warnings from sqlite3_log()
	case warning(WarningExtendedResultCode? = nil)
	/// `sqlite3_step()` has another row ready
	case row
	/// `sqlite3_step()` has finished executing
	case done

	// Extended result codes

	/// SQLITE_OK_* extended result codes
	public enum OKExtendedResultCode {
		/// SQLITE_OK_LOAD_PERMANENTLY
		case loadPermanently
		/// SQLITE_OK_LOAD_SYMLINK
		case symLink
	}

	/// SQLITE_ERROR_* extended result codes
	public enum ErrorExtendedResultCode {
		/// SQLITE_ERROR_MISSING_COLLSEQ
		case missingCollSeq
		/// SQLITE_ERROR_RETRY
		case retry
		/// SQLITE_ERROR_SNAPSHOT
		case snapshot
	}

	/// SQLITE_ABORT_* extended result codes
	public enum AbortExtendedResultCode {
		/// SQLITE_ABORT_ROLLBACK
		case rollback
	}

	/// SQLITE_BUSY_* extended result codes
	public enum BusyExtendedResultCode {
		/// SQLITE_BUSY_RECOVERY
		case recovery
		/// SQLITE_BUSY_SNAPSHOT
		case snapshot
		/// SQLITE_BUSY_TIMEOUT
		case timeout
	}

	/// SQLITE_LOCKED_* extended result codes
	public enum LockedExtendedResultCode {
		/// SQLITE_LOCKED_SHAREDCACHE
		case sharedCache
		/// SQLITE_LOCKED_VTAB
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
		/// SQLITE_READONLY_CANTINIT
		case cantInit
		/// SQLITE_READONLY_DIRECTORY
		case directory
	}

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
		/// SQLITE_IOERR_ROLLBACK_DATA
		case data
	}

	/// SQLITE_CORRUPT_* extended result codes
	public enum CorruptExtendedResultCode {
		/// SQLITE_CORRUPT_VTAB
		case vtab
		/// SQLITE_CORRUPT_SEQUENCE
		case sequence
		/// SQLITE_CORRUPT_INDEX
		case index
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
		/// SQLITE_CANTOPEN_DIRTYWAL
		case dirtyWAL /* Not Used */
		/// SQLITE_CANTOPEN_SYMLINK
		case symLink
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
		/// SQLITE_CONSTRAINT_PINNED
		case pinned
	}

	/// SQLITE_AUTH_* extended result codes
	public enum AuthExtendedResultCode {
		/// SQLITE_AUTH_USER
		case user
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
}

extension SQLiteResult {
	init(_ code: Int32) {
		let primary = code & 0xff
		let extended = code >> 8
		switch primary {
		case SQLITE_OK:
			switch extended {
			case 1:					self = .ok(.loadPermanently)
			case 2:					self = .ok(.symLink)
			default:				self = .ok(nil)
			}
		case SQLITE_ERROR:
			switch extended {
			case 1: 				self = .error(.missingCollSeq)
			case 2: 				self = .error(.retry)
			case 3: 				self = .error(.snapshot)
			default: 				self = .error(nil)
			}
		case SQLITE_INTERNAL:		self = .`internal`
		case SQLITE_PERM:			self = .perm
		case SQLITE_ABORT:
			switch extended {
			case 2: 				self = .abort(.rollback)
			default:				self = .abort(nil)
			}
		case SQLITE_BUSY:
			switch extended {
			case 1: 				self = .busy(.recovery)
			case 2: 				self = .busy(.snapshot)
			case 3: 				self = .busy(.timeout)
			default:				self = .busy(nil)
			}
		case SQLITE_LOCKED:
			switch extended {
			case 1: 				self = .locked(.sharedCache)
			case 2: 				self = .locked(.vtab)
			default:				self = .locked(nil)
			}
		case SQLITE_NOMEM:			self = .noMem
		case SQLITE_READONLY:
			switch extended {
			case 1: 				self = .readOnly(.recovery)
			case 2: 				self = .readOnly(.cantLock)
			case 3: 				self = .readOnly(.rollback)
			case 4: 				self = .readOnly(.dbMoved)
			case 5: 				self = .readOnly(.cantInit)
			case 6: 				self = .readOnly(.directory)
			default:				self = .readOnly(nil)
			}
		case SQLITE_INTERRUPT:		self = .interrupt
		case SQLITE_IOERR:
			switch extended {
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
			case 32:				self = .ioErr(.data)
			default:				self = .ioErr(nil)
			}
		case SQLITE_CORRUPT:
			switch extended {
			case 1: 				self = .corrupt(.vtab)
			case 2: 				self = .corrupt(.sequence)
			case 3: 				self = .corrupt(.index)
			default:				self = .corrupt(nil)
			}
		case SQLITE_NOTFOUND:		self = .notFound
		case SQLITE_FULL:			self = .full
		case SQLITE_CANTOPEN:
			switch extended {
			case 1: 				self = .cantOpen(.noTempDir)
			case 2: 				self = .cantOpen(.isDir)
			case 3: 				self = .cantOpen(.fullPath)
			case 4: 				self = .cantOpen(.convPath)
			case 5: 				self = .cantOpen(.dirtyWAL) /* Not Used */
			case 6: 				self = .cantOpen(.symLink)
			default:				self = .cantOpen(nil)
			}
		case SQLITE_PROTOCOL:		self = .`protocol`
		case SQLITE_EMPTY:			self = .empty
		case SQLITE_SCHEMA:			self = .schema
		case SQLITE_TOOBIG:			self = .tooBig
		case SQLITE_CONSTRAINT:
			switch extended {
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
			case 11:				self = .constraint(.pinned)
			default:				self = .constraint(nil)
			}
		case SQLITE_MISMATCH:		self = .mismatch
		case SQLITE_MISUSE:			self = .misuse
		case SQLITE_NOLFS:			self = .noLFS
		case SQLITE_AUTH:
			switch extended {
			case 1:					self = .auth(.user)
			default:				self = .auth(nil)
			}
		case SQLITE_FORMAT:			self = .format
		case SQLITE_RANGE:			self = .range
		case SQLITE_NOTADB:			self = .notADB
		case SQLITE_NOTICE:
			switch extended {
			case 1:					self = .notice(.recoverWAL)
			case 2:					self = .notice(.recoverRollback)
			default:				self = .notice(nil)
			}
		case SQLITE_WARNING:
			switch extended {
			case 1:					self = .warning(.autoindex)
			default:				self = .warning(nil)
			}
		case SQLITE_ROW:			self = .row
		case SQLITE_DONE:			self = .done

		default:
			preconditionFailure("Unknown SQLite result code")
		}
	}

	var code: Int32 {
		switch self {
		case .ok(let extended):
			if let extended = extended {
				switch extended {
				case .loadPermanently:		return SQLITE_OK | (1 << 8)
				case .symLink:				return SQLITE_OK | (2 << 8)
				}
			}
			return SQLITE_OK
		case .error:
			return SQLITE_ERROR
		case .internal:
			return SQLITE_INTERNAL
		case .perm:
			return SQLITE_PERM
		case .abort(let extended):
			if let extended = extended {
				switch extended {
				case .rollback:				return SQLITE_ABORT | (1 << 8)
				}
			}
			return SQLITE_ABORT
		case .busy(let extended):
			if let extended = extended {
				switch extended {
				case .recovery:				return SQLITE_BUSY | (1 << 8)
				case .snapshot:				return SQLITE_BUSY | (2 << 8)
				case .timeout:				return SQLITE_BUSY | (3 << 8)
				}
			}
			return SQLITE_BUSY
		case .locked(let extended):
			if let extended = extended {
				switch extended {
				case .sharedCache:			return SQLITE_LOCKED | (1 << 8)
				case .vtab:					return SQLITE_LOCKED | (2 << 8)
				}
			}
			return SQLITE_LOCKED
		case .noMem:
			return SQLITE_NOMEM
		case .readOnly(let extended):
			if let extended = extended {
				switch extended {
				case .recovery:				return SQLITE_READONLY | (1 << 8)
				case .cantLock:				return SQLITE_READONLY | (2 << 8)
				case .rollback:				return SQLITE_READONLY | (3 << 8)
				case .dbMoved:				return SQLITE_READONLY | (4 << 8)
				case .cantInit:				return SQLITE_READONLY | (5 << 8)
				case .directory:			return SQLITE_READONLY | (6 << 8)
				}
			}
			return SQLITE_READONLY
		case .interrupt:
			return SQLITE_INTERRUPT
		case .ioErr(let extended):
			if let extended = extended {
				switch extended {
				case .read:					return SQLITE_IOERR | (1 << 8)
				case .shortRead:			return SQLITE_IOERR | (2 << 8)
				case .write:				return SQLITE_IOERR | (3 << 8)
				case .fSync:				return SQLITE_IOERR | (4 << 8)
				case .dirFSync:				return SQLITE_IOERR | (5 << 8)
				case .truncate:				return SQLITE_IOERR | (6 << 8)
				case .fStat:				return SQLITE_IOERR | (7 << 8)
				case .unlock:				return SQLITE_IOERR | (8 << 8)
				case .rdLock:				return SQLITE_IOERR | (9 << 8)
				case .delete:				return SQLITE_IOERR | (10 << 8)
				case .blocked:				return SQLITE_IOERR | (11 << 8)
				case .noMem:				return SQLITE_IOERR | (12 << 8)
				case .access:				return SQLITE_IOERR | (13 << 8)
				case .checkReservedLock:	return SQLITE_IOERR | (14 << 8)
				case .lock:					return SQLITE_IOERR | (15 << 8)
				case .close:				return SQLITE_IOERR | (16 << 8)
				case .dirClose:				return SQLITE_IOERR | (17 << 8)
				case .shmOpen:				return SQLITE_IOERR | (18 << 8)
				case .shmSize:				return SQLITE_IOERR | (19 << 8)
				case .shmLock:				return SQLITE_IOERR | (20 << 8)
				case .shmMap:				return SQLITE_IOERR | (21 << 8)
				case .seek:					return SQLITE_IOERR | (22 << 8)
				case .deleteNoEnt:			return SQLITE_IOERR | (23 << 8)
				case .mmap:					return SQLITE_IOERR | (24 << 8)
				case .getTempPath:			return SQLITE_IOERR | (25 << 8)
				case .convPath:				return SQLITE_IOERR | (26 << 8)
				case .vnode:				return SQLITE_IOERR | (27 << 8)
				case .auth:					return SQLITE_IOERR | (28 << 8)
				case .beginAtomic:			return SQLITE_IOERR | (29 << 8)
				case .commitAtomic:			return SQLITE_IOERR | (30 << 8)
				case .rollbackAtomic:		return SQLITE_IOERR | (31 << 8)
				case .data:					return SQLITE_IOERR | (32 << 8)
				}
			}
			return SQLITE_IOERR
		case .corrupt(let extended):
			if let extended = extended {
				switch extended {
				case .vtab:					return SQLITE_CORRUPT | (1 << 8)
				case .sequence:				return SQLITE_CORRUPT | (2 << 8)
				case .index:				return SQLITE_CORRUPT | (3 << 8)
				}
			}
			return SQLITE_CORRUPT
		case .notFound:
			return SQLITE_NOTFOUND
		case .full:
			return SQLITE_FULL
		case .cantOpen(let extended):
			if let extended = extended {
				switch extended {
				case .noTempDir:			return SQLITE_CANTOPEN | (1 << 8)
				case .isDir:				return SQLITE_CANTOPEN | (2 << 8)
				case .fullPath:				return SQLITE_CANTOPEN | (3 << 8)
				case .convPath:				return SQLITE_CANTOPEN | (4 << 8)
				case .dirtyWAL:				return SQLITE_CANTOPEN | (5 << 8) /* Not Used */
				case .symLink:				return SQLITE_CANTOPEN | (6 << 8)
				}
			}
			return SQLITE_CANTOPEN
		case .protocol:
			return SQLITE_PROTOCOL
		case .empty:
			return SQLITE_EMPTY
		case .schema:
			return SQLITE_SCHEMA
		case .tooBig:
			return SQLITE_TOOBIG
		case .constraint(let extended):
			if let extended = extended {
				switch extended {
				case .check:				return SQLITE_CONSTRAINT | (1 << 8)
				case .commitHook:			return SQLITE_CONSTRAINT | (2 << 8)
				case .foreignKey:			return SQLITE_CONSTRAINT | (3 << 8)
				case .function:				return SQLITE_CONSTRAINT | (4 << 8)
				case .notNull:				return SQLITE_CONSTRAINT | (5 << 8)
				case .primaryKey:			return SQLITE_CONSTRAINT | (6 << 8)
				case .trigger:				return SQLITE_CONSTRAINT | (7 << 8)
				case .unique:				return SQLITE_CONSTRAINT | (8 << 8)
				case .vtab:					return SQLITE_CONSTRAINT | (9 << 8)
				case .rowid:				return SQLITE_CONSTRAINT | (10 << 8)
				case .pinned:				return SQLITE_CONSTRAINT | (11 << 8)
				}
			}
			return SQLITE_CONSTRAINT
		case .mismatch:
			return SQLITE_MISMATCH
		case .misuse:
			return SQLITE_MISUSE
		case .noLFS:
			return SQLITE_NOLFS
		case .auth(let extended):
			if let extended = extended {
				switch extended {
				case .user:					return SQLITE_AUTH | (1 << 8)
				}
			}
			return SQLITE_AUTH
		case .format:
			return SQLITE_FORMAT
		case .range:
			return SQLITE_RANGE
		case .notADB:
			return SQLITE_NOTADB
		case .notice(let extended):
			if let extended = extended {
				switch extended {
				case .recoverWAL:			return SQLITE_NOTICE | (1 << 8)
				case .recoverRollback:		return SQLITE_NOTICE | (2 << 8)
				}
			}
			return SQLITE_NOTICE
		case .warning(let extended):
			if let extended = extended {
				switch extended {
				case .autoindex:			return SQLITE_WARNING | (1 << 8)
				}
			}
			return SQLITE_WARNING
		case .row:
			return SQLITE_ROW
		case .done:
			return SQLITE_DONE
		}
	}
}
