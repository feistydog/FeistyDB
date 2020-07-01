//
// Copyright (c) 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import os.log
import Foundation
import CSQLite

#if SQLITE_ENABLE_PREUPDATE_HOOK && SQLITE_ENABLE_SESSION

/// An `sqlite3_session *` object.
///
/// - seealso: [The Session Extension](https://www.sqlite.org/sessionintro.html)
/// - seealso: [Introduction](https://www.sqlite.org/session/intro.html)
//public typealias SQLiteSession = UnsafeMutablePointer<sqlite3_session>
public typealias SQLiteSession = OpaquePointer

/// A mechanism for recording changes to some or all tables in a database.
///
/// - seealso: [The Session Extension](https://www.sqlite.org/sessionintro.html)
/// - seealso: [Introduction](https://www.sqlite.org/session/intro.html)
public final class Session {
	/// The owning database
	public let database: Database

	/// The underlying `sqlite3_session *` object
	let session: SQLiteSession

	/// Initializes a new session for `schema` on `database`
	///
	/// - parameter database: The owning database
	/// - parameter schema: The database schema to track
	///
	/// - throws: An error if the session could not be created
	init(database: Database, schema: String) throws {
		self.database = database

		var session: SQLiteSession? = nil
		let rc = sqlite3session_create(database.db, schema, &session)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error creating database session for schema \"\(schema)\"", code: rc)
		}

		self.session = session!
	}

	deinit {
		sqlite3session_delete(session)
	}

	/// The indirect change flag
	///
	/// - seealso: [Set Or Clear the Indirect Change Flag](https://www.sqlite.org/session/sqlite3session_indirect.html)
	var indirect: Bool {
		get {
			sqlite3session_indirect(session, -1) != 0
		}
		set {
			_ = sqlite3session_indirect(session, newValue ? 1 : 0)
		}
	}

	/// Returns `true` if the session contains no changes.
	///
	/// - seealso: [Test if a changeset has recorded any changes.](https://www.sqlite.org/session/sqlite3session_isempty.html)
	var isEmpty: Bool {
		sqlite3session_isempty(session) != 0
	}

	/// Attaches a table to the session.
	///
	/// - parameter table: The name of the table to attach
	///
	/// - throws: An error if the table could not be attached to the session
	/// 
	/// - seealso: [Attach A Table To A Session Object](https://www.sqlite.org/session/sqlite3session_attach.html)
	func attach(_ table: String) throws {
		let rc = sqlite3session_attach(session, table)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error attaching table \"\(table)\" to database session", code: rc)
		}
	}

	/// Attaches all tables to the session.
	///
	/// - throws: An error if the tables could not be attached to the session
	///
	/// - seealso: [Attach A Table To A Session Object](https://www.sqlite.org/session/sqlite3session_attach.html)
	func attachAll() throws {
		let rc = sqlite3session_attach(session, nil)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error attaching all tables to database session", code: rc)
		}
	}

	/// Creates and returns a changeset.
	///
	/// - throws: An error if the changeset could not be created
	func changeset() throws -> Changeset {
		var changeset: SQLiteChangeset? = nil
		var size: Int32 = 0
		defer {
			sqlite3_free(changeset)
		}

		let rc = sqlite3session_changeset(session, &size, &changeset)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error creating changeset for database session", code: rc)
		}

		// Tables with no PK return 0 for size and NULL for changeset
		let data = size > 0 ? Data(bytes: changeset!, count: Int(size)) : Data()
		return Changeset(data: data)
	}
}

/// A changeset object.
typealias SQLiteChangeset = UnsafeMutableRawPointer

/// A changeset
///
/// - seealso: [Generate A Changeset From A Session Object](https://www.sqlite.org/session/sqlite3session_changeset.html)
public struct Changeset {
	/// The raw changeset data
	public let data: Data

	/// Returns an inverted version of `self`
	///
	/// - throws: An error if the changeset could not be inverted
	///
	/// - seealso: [Invert A Changeset](https://www.sqlite.org/session/sqlite3changeset_invert.html)
	public func inverted() throws -> Changeset {
		var changeset: SQLiteChangeset? = nil
		var size: Int32 = 0
		defer {
			sqlite3_free(changeset)
		}

		let rc = data.withUnsafeBytes { buf in
			sqlite3changeset_invert(Int32(data.count), buf.baseAddress, &size, &changeset)
		}
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error inverting changeset", code: rc)
		}

		let inverted_data = size > 0 ? Data(bytes: changeset!, count: Int(size)) : Data()
		return Changeset(data: inverted_data)
	}

	/// Appends the changes in `other` to self
	///
	/// - parameter other: A changset containing the changes to append
	///
	/// - throws: An error if the changeset could not be appended
	///
	/// - seealso: [Concatenate Two Changeset Objects](https://www.sqlite.org/session/sqlite3changeset_concat.html)
	public func appending(_ other: Changeset) throws -> Changeset {
		var changeset: SQLiteChangeset? = nil
		var size: Int32 = 0
		defer {
			sqlite3_free(changeset)
		}

		let rc = data.withUnsafeBytes { buf -> Int32 in
			let ptr = UnsafeMutableRawPointer(mutating: buf.baseAddress)
			return other.data.withUnsafeBytes { other_buf in
				let other_ptr = UnsafeMutableRawPointer(mutating: other_buf.baseAddress)
				return sqlite3changeset_concat(Int32(data.count), ptr, Int32(other.data.count), other_ptr, &size, &changeset)
			}
		}
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error appending changeset", code: rc)
		}

		let concat_data = size > 0 ? Data(bytes: changeset!, count: Int(size)) : Data()
		return Changeset(data: concat_data)
	}

	/// Invokes `block` with each operation in the changeset.
	///
	/// - parameter block: A closure applied to each changeset operation
	/// - parameter operation: A changeset operation
	///
	/// - throws: Any error thrown in `block` or an error if the iteratation did not successfully run to completion
	public func operations(_ options: ChangesetIterator.Options = [], _ block: ((_ operation: ChangesetOperation) throws -> ())) throws {
		let iterator = try ChangesetIterator(self, options)
		var operation = try iterator.nextOperation()
		while operation != nil {
			try block(operation!)
			operation = try iterator.nextOperation()
		}
	}
}

/// A changeset iterator object.
//public typealias SQLiteChangesetIterator = UnsafeMutablePointer<sqlite3_changeset_iter>
public typealias SQLiteChangesetIterator = OpaquePointer

/// An operation in a changeset
public struct ChangesetOperation {
	/// The raw iterator
	let iterator: SQLiteChangesetIterator
	/// Whether this operation contains conflicting value information
	let isConflict: Bool

	init(_ iterator: SQLiteChangesetIterator, _ isConflict: Bool = false) throws {
		var table_name: UnsafePointer<Int8>? = nil
		var cols: Int32 = 0
		var op: Int32 = 0
		var indirect: Int32 = 0
		let rc = sqlite3changeset_op(iterator, &table_name, &cols, &op, &indirect)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error obtaining current changeset operation", code: rc)
		}

		self.iterator = iterator
		self.isConflict = isConflict
		self.table = String(utf8String: table_name.unsafelyUnwrapped).unsafelyUnwrapped
		self.columnCount = Int(cols)
		self.operation = Database.RowChangeType(op)
		self.indirect = indirect != 0
	}

	/// The name of the table
	public let table: String
	/// The number of columns in `table`
	public let columnCount: Int
	/// The operation being performed
	public let operation: Database.RowChangeType
	/// True for an indirect change
	public let indirect: Bool

	/// Returns the old value for the column at `index` in the current change
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.count`
	///
	/// - note: This is only valid for `.update` and `.delete` row change types
	///
	/// - parameter index: The index of the desired column
	///
	/// - returns: The old value for column `index`
	///
	/// - throws: An error if `index` is out of bounds or an other error occurs
	public func oldValue(at index: Int) throws -> DatabaseValue {
		var value: SQLiteValue?
		let rc = sqlite3changeset_old(iterator, Int32(index), &value)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Unable to retrieve old value for change", code: rc)
		}
		return DatabaseValue(value.unsafelyUnwrapped)
	}

	/// Returns the new value for the column at `index` in the current change
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.count`
	///
	/// - note: This is only valid for `.update` and `.insert` row change types
	///
	/// - parameter index: The index of the desired column
	///
	/// - returns: The new value for column `index`
	///
	/// - throws: An error if `index` is out of bounds or an other error occurs
	public func newValue(at index: Int) throws -> DatabaseValue {
		var value: SQLiteValue?
		let rc = sqlite3changeset_new(iterator, Int32(index), &value)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Unable to retrieve new value for change", code: rc)
		}
		return DatabaseValue(value.unsafelyUnwrapped)
	}

	/// Returns the conflicting value for the column at `index` in the current change
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.count`
	///
	/// - note: Unless invoked from an object passed to a conflict handler `nil` is returned
	///
	/// - parameter index: The index of the desired column
	///
	/// - returns: The conflicting value for column `index` or `nil` if no conflict
	///
	/// - throws: An error if `index` is out of bounds or an other error occurs
	public func conflictingValue(at index: Int) throws -> DatabaseValue? {
		guard isConflict else {
			return nil
		}

		var value: SQLiteValue?
		let rc = sqlite3changeset_conflict(iterator, Int32(index), &value)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Unable to retrieve conflicting value for change", code: rc)
		}
		return DatabaseValue(value.unsafelyUnwrapped)
	}

	/// Returns the primary key definition of a table
	///
	/// - returns: An array corresponding to the table columns where `true` indicates if that column is part of the table's primary key
	///
	/// - throws: An error if the changeset operation is invalid
	///
	/// - seealso: [Obtain The Primary Key Definition Of A Table](https://www.sqlite.org/session/sqlite3changeset_pk.html)
	public func primaryKey() throws -> [Bool] {
		var pk: UnsafeMutablePointer<UInt8>? = nil
		var cols: Int32 = 0
		let rc = sqlite3changeset_pk(iterator, &pk, &cols)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Unable to retrieve primary key for change", code: rc)
		}
		var result: [Bool] = []
		for i in 0..<Int(cols) {
			result.append(pk.unsafelyUnwrapped[i] != 0)
		}
		return result
	}
}

/// An iterator over a changeset
public final class ChangesetIterator {
	/// Flags affecting changeset iteration
	///
	/// - seealso: [Flags for sqlite3changeset_start_v2](https://www.sqlite.org/session/c_changesetstart_invert.html)
	public struct Options: OptionSet {
		public let rawValue: Int32

		public init(rawValue: Int32) {
			self.rawValue = rawValue
		}

		/// Invert the changeset while iterating through it.
		public static let invert = Options(rawValue: SQLITE_CHANGESETSTART_INVERT)
	}

	/// The underlying iterator
	var iterator: SQLiteChangesetIterator

	init(_ changeset: Changeset, _ options: Options) throws {
		var iterator: OpaquePointer?
		let rc = changeset.data.withUnsafeBytes { buf -> Int32 in
			let ptr = UnsafeMutableRawPointer(mutating: buf.baseAddress)
			return sqlite3changeset_start_v2(&iterator, Int32(changeset.data.count), ptr, options.rawValue)
		}
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error starting changeset iteration", code: rc)
		}
		self.iterator = iterator!
	}

	deinit {
		sqlite3changeset_finalize(iterator)
	}

	/// Advances the iterator and returns the next changeset operation or `nil` if none
	///
	/// - seealso: [Obtain The Current Operation From A Changeset Iterator](https://www.sqlite.org/session/sqlite3changeset_op.html)
	public func nextOperation() throws -> ChangesetOperation? {
		let rc = sqlite3changeset_next(iterator)
		switch rc {
		case SQLITE_ROW:
			return try ChangesetOperation(iterator)
		case SQLITE_DONE:
			return nil
		default:
			throw SQLiteError("Error advancing changeset iterator", code: rc)
		}
	}
}

extension ChangesetIterator: IteratorProtocol {
	/// Returns the next changeset operation or `nil` if none.
	///
	/// Because the iterator discards errors, the preferred way of accessing changeset operations
	/// is via `nextOperation()` or `operations(_:)`
	///
	/// - returns: The next changeset operation
	public func next() -> ChangesetOperation? {
		return try? nextOperation()
	}
}

/// A changegroup object.
//public typealias SQLiteChangegroup = UnsafeMutablePointer<sqlite3_changegroup>
public typealias SQLiteChangegroup = OpaquePointer

/// A mechanism for grouping changesets.
///
/// - seealso: [Create A New Changegroup Object](https://www.sqlite.org/session/sqlite3changegroup_new.html)
public final class Changegroup {
	/// The underlying `sqlite3_changegroup *` object
	let changegroup: SQLiteChangegroup

	/// Initializes a new changegroup
	///
	/// - throws: An error if the changegroup could not be created
	init() throws {
		var changegroup: SQLiteChangegroup? = nil
		let rc = sqlite3changegroup_new(&changegroup)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error creating changegroup", code: rc)
		}

		self.changegroup = changegroup!
	}

	deinit {
		sqlite3changegroup_delete(changegroup)
	}

	/// Adds the changes in `changeset` to `self`.
	///
	/// - parameter changeset: A changset containing the changes to add
	///
	/// - throws: An error if the changeset could not be added
	///
	/// - seealso: [Add A Changeset To A Changegroup](https://www.sqlite.org/session/sqlite3changegroup_add.html)
	public func add(_ changeset: Changeset) throws  {
		let rc = changeset.data.withUnsafeBytes { buf -> Int32 in
			let ptr = UnsafeMutableRawPointer(mutating: buf.baseAddress)
			return sqlite3changegroup_add(changegroup, Int32(changeset.data.count), ptr)
		}
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error adding changeset to changegroup", code: rc)
		}
	}

	/// Creates and returns a changeset containing the changes in `self`.
	///
	/// - throws: An error if the changeset could not be created
	func changeset() throws -> Changeset {
		var changeset: SQLiteChangeset? = nil
		var size: Int32 = 0
		defer {
			sqlite3_free(changeset)
		}

		let rc = sqlite3changegroup_output(changegroup, &size, &changeset)
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error creating changeset for changegroup", code: rc)
		}

		let data = size > 0 ? Data(bytes: changeset!, count: Int(size)) : Data()
		return Changeset(data: data)
	}
}

/// A rebaser object.
typealias SQLiteRebaser = UnsafeMutableRawPointer

/// A rebaser
struct Rebaser {
	/// The raw rebaser data
	public let data: Data
}

/// A closure specifying whether `table` should be included when applying a changeset.
///
/// - parameter table: The name of the table to test for inclusion
///
/// - returns: `true` if `table` should be included in the operation, `false` otherwise
public typealias ChangesetTableFilter = (_ table: String) -> Bool

/// Possible results for a changeset conflict handler.
///
/// - seealso: [Constants Returned By The Conflict Handler](https://www.sqlite.org/session/c_changeset_abort.html)
public enum ChangesetConflictHandlerResult {
	/// The change that caused the conflict is not applied
	case omit
	/// The conflicting row is replaced
	case replace
	/// Any changes applied are rolled back and the changeset application is aborted
	case abort
}

/// Possible conflicts passed to a changeset conflict handler.
///
/// - seealso: [Constants Passed To The Conflict Handler](https://www.sqlite.org/session/c_changeset_conflict.html)
public enum ChangesetConflictHandlerConflict {
	/// One or more primary key fields do not contain the expected "before" data
	case data(ChangesetOperation)
	/// The required primary key field was not found in the database
	case notFound(ChangesetOperation)
	/// The operation would result in duplicate primary key values
	case conflict(ChangesetOperation)
	/// A constraint violation occurred
	case constraint
	/// The database is left in a state containing foreign key violations
	case foreignKey(Int)
}

/// A changeset conflict handler
///
/// - parameter conflict: A conflict that occurred during changeset application
///
/// - returns: The action that should be taken to resolve the conflict
public typealias ChangesetConflictHandler = (_ conflict: ChangesetConflictHandlerConflict) -> ChangesetConflictHandlerResult

/// Flags affecting application of changesets
///
/// - seealso: [Flags for sqlite3changeset_apply_v2](https://www.sqlite.org/session/c_changesetapply_invert.html)
public struct ChangesetApplyOptions: OptionSet {
	public let rawValue: Int32

	public init(rawValue: Int32) {
		self.rawValue = rawValue
	}

	/// Do not wrap changeset application in a `SAVEPOINT`
	public static let noSavepoint = ChangesetApplyOptions(rawValue: SQLITE_CHANGESETAPPLY_NOSAVEPOINT)
	/// Invert the changeset before applying it.
	public static let invert = ChangesetApplyOptions(rawValue: SQLITE_CHANGESETAPPLY_INVERT)
}

extension Database {
	/// Applies a changeset to the database.
	///
	/// - parameter changeset: The changeset to apply
	/// - parameter options: Options affecting how the changeset is applied
	/// - parameter isIncluded: An optional closure returning `true` if changes to the named table should be applied. If `nil`, all tables will be included.
	/// - parameter onConflict: A closure indicating how conflicts should be handled
	///
	/// - throws: An error if the changeset could not be applied
	///
	/// - seealso: [Apply A Changeset To A Database](https://www.sqlite.org/session/sqlite3changeset_apply.html)
	public func apply(_ changeset: Changeset, options: ChangesetApplyOptions = [], _ isIncluded: ChangesetTableFilter? = nil, _ onConflict: @escaping ChangesetConflictHandler) throws {
		var rebaser: SQLiteRebaser? = nil
		var size: Int32 = 0

		struct ChangesetApplyContext {
			let isIncluded: ChangesetTableFilter?
			let onConflict: ChangesetConflictHandler
		}

		let context = ChangesetApplyContext(isIncluded: isIncluded, onConflict: onConflict)
		let context_ptr = UnsafeMutablePointer<ChangesetApplyContext>.allocate(capacity: 1)
		context_ptr.initialize(to: context)
		defer {
			context_ptr.deinitialize(count: 1)
			context_ptr.deallocate()
		}

		let rc = changeset.data.withUnsafeBytes { buf -> Int32 in
			let ptr = UnsafeMutableRawPointer(mutating: buf.baseAddress)
			return sqlite3changeset_apply_v2(db, Int32(changeset.data.count), ptr, { (context, table_name) -> Int32 in
				let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: ChangesetApplyContext.self)

				guard let isIncluded = context_ptr.pointee.isIncluded else {
					return 1
				}

				let ptr = UnsafeMutableRawPointer(mutating: table_name.unsafelyUnwrapped)
				let table = String(bytesNoCopy: ptr, length: strlen(table_name.unsafelyUnwrapped), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
//				let table = String(utf8String: table_name.unsafelyUnwrapped).unsafelyUnwrapped
				return isIncluded(table) ? 1 : 0
			}, { (context, raw_conflict, iter) -> Int32 in
				let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: ChangesetApplyContext.self)
				let onConflict = context_ptr.pointee.onConflict

				let conflict: ChangesetConflictHandlerConflict
				do {
					switch raw_conflict {
					case SQLITE_CHANGESET_DATA:
						let operation = try ChangesetOperation(iter.unsafelyUnwrapped, true)
						conflict = .data(operation)
					case SQLITE_CHANGESET_NOTFOUND:
						let operation = try ChangesetOperation(iter.unsafelyUnwrapped)
						conflict = .notFound(operation)
					case SQLITE_CHANGESET_CONFLICT:
						let operation = try ChangesetOperation(iter.unsafelyUnwrapped, true)
						conflict = .conflict(operation)
					case SQLITE_CHANGESET_CONSTRAINT:
						conflict = .constraint
					case SQLITE_CHANGESET_FOREIGN_KEY:
						var fk_conflicts: Int32 = 0
						guard sqlite3changeset_fk_conflicts(iter.unsafelyUnwrapped, &fk_conflicts) == SQLITE_OK else {
							return SQLITE_CHANGESET_ABORT
						}
						conflict = .foreignKey(Int(fk_conflicts))
					default:
						preconditionFailure("Unexpected conflict type")
					}
				}

				catch let error {
					os_log("Error processing changeset conflict: %{public}@", type: .info, (error as? SQLiteError)?.description ?? error.localizedDescription)
					return SQLITE_CHANGESET_ABORT
				}

				let result = onConflict(conflict)
				return result.rawValue
			}, context_ptr, &rebaser, &size, options.rawValue)
		}
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error applying changeset", code: rc)
		}

		// Rebaser is experimental
		if rebaser != nil {
			let data = size > 0 ? Data(bytes: rebaser!, count: Int(size)) : Data()
			/*return */_ = Rebaser(data: data)
		}
	}
}

extension ChangesetConflictHandlerResult: RawRepresentable {
	public init?(rawValue: Int32) {
		switch rawValue {
		case SQLITE_CHANGESET_OMIT: 	self = .omit
		case SQLITE_CHANGESET_REPLACE: 	self = .replace
		case SQLITE_CHANGESET_ABORT: 	self = .abort
		default:						return nil
		}
	}

	public var rawValue: Int32 {
		switch self {
		case .omit: 		return SQLITE_CHANGESET_OMIT
		case .replace: 		return SQLITE_CHANGESET_REPLACE
		case .abort: 		return SQLITE_CHANGESET_ABORT
		}
	}
}

#endif
