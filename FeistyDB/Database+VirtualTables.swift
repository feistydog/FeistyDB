//
// Copyright (c) 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import os.log
import Foundation

/// A cursor for an SQLite virtual table
public protocol VirtualTableCursor {
	/// Returns the value of  column `index` in the row at which the cursor is pointing
	///
	/// - note: Column indexes are 0-based
	///
	/// - parameter index: The desired column
	///
	/// - returns: The value of column `index` in the current row
	///
	/// - throws: `SQLiteError` if an error occurs
	func column(_ index: Int32) throws -> DatabaseValue

	/// Advances the cursor to the next row of output
	///
	/// - throws: `SQLiteError` if an error occurs
	func next() throws

	/// Returns the rowid for the current row
	///
	/// - returns: The rowid of the current row
	///
	/// - throws: `SQLiteError` if an error occurs
	func rowid() throws -> Int64

	/// Applies a filter to the virtual table
	///
	/// - parameter arguments: Arguments applicable to the query plan made in `VirtualTableModule.bestIndex()`
	/// - parameter indexNumber: The index number returned by `VirtualTableModule.bestIndex()`
	/// - parameter indexName: The index name returned by `VirtualTableModule.bestIndex()`
	///
	/// - throws: `SQLiteError` if an error occurs
	func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) throws

	/// `true` if the cursor has been moved off the last row of output
	var eof: Bool { get }
}

/// Possible results for the `VirtualTableModule.bestIndex()` function
public enum VirtualTableModuleBestIndexResult {
	/// Success
	case ok
	/// No usable query plan exists
	case constraint
}

/// An SQLite virtual table module
public protocol VirtualTableModule {
	/// Initializes an SQLite virtual table module.
	///
	/// - parameter arguments: The arguments used to create the virtual table module.
	///
	/// - throws: `SQLiteError` if the module could not be created
	init(arguments: [String]) throws

	/// The options supported by this virtual table module
	var options: Database.VirtualTableModuleOptions { get }

	/// The SQL `CREATE TABLE` statement used to tell SQLite about the virtual table's columns and datatypes.
	///
	/// - note: The name of the table and any constraints are ignored.
	var declaration: String { get }

	/// Determines the query plan to use for a given query
	///
	/// - parameter indexInfo: An `sqlite3_index_info` struct containing information on the query
	///
	/// - returns: `.ok` on success or `.constraint` if the configuration of unusable flags in `indexInfo` cannot result in a usable query plan
	///
	/// - throws: `SQLiteError` if an error occurs
	func bestIndex(_ indexInfo: inout sqlite3_index_info) throws -> VirtualTableModuleBestIndexResult

	/// Opens and returns a cursor for the virtual table
	///
	/// - returns: An initalized cursor for the virtual table
	///
	/// - throws: `SQLiteError` error if the cursor could not be created
	func openCursor() throws -> VirtualTableCursor
}

extension VirtualTableModule {
	var options: Database.VirtualTableModuleOptions {
		return []
	}
}

extension Database {
	/// Glue for creating a generic Swift type in a C callback
	final class VirtualTableModuleClientData {
		/// The constructor closure
		let construct: (_ arguments : [String]) throws -> VirtualTableModule

		/// Persistent sqlite3_module instance
		let module: UnsafeMutablePointer<sqlite3_module>

		/// Creates client data for a module
		init(module: inout sqlite3_module, _ construct: @escaping (_ arguments: [String]) throws -> VirtualTableModule) {
			let module_ptr = UnsafeMutablePointer<sqlite3_module>.allocate(capacity: 1)
			module_ptr.assign(from: &module, count: 1)
			self.module = module_ptr
			self.construct = construct
		}

		deinit {
			module.deallocate()
		}
	}

	/// Virtual table module options
	///
	/// - seealso: [Virtual Table Configuration Options](https://sqlite.org/c3ref/c_vtab_constraint_support.html)
	public struct VirtualTableModuleOptions: OptionSet {
		public let rawValue: Int

		public init(rawValue: Int) {
			self.rawValue = rawValue
		}

		/// Indicates whether the virtual table module supports constraints
		public static let constraintSupport = VirtualTableModuleOptions(rawValue: 1 << 0)
		/// The virtual table module is unlikely to cause problems even if misused.
		public static let innocuous = VirtualTableModuleOptions(rawValue: 1 << 2)
		/// The virtual table module is prohibited from use in triggers or views
		public static let directOnly = VirtualTableModuleOptions(rawValue: 1 << 3)
	}

	/// Adds a virtual table module to the database.
	///
	/// For example, a virtual table module returning the natural numbers could be implemented as:
	/// ```swift
	/// class NaturalNumbersModule: VirtualTableModule {
	/// 	class Cursor: VirtualTableCursor {
	/// 		var _rowid: Int64 = 0
	///
	/// 		func column(_ index: Int32) -> DatabaseValue  {
	/// 			.integer(_rowid)
	/// 		}
	///
	/// 		func next() {
	/// 			_rowid += 1
	/// 		}
	///
	/// 		func rowid() -> Int64 {
	/// 			_rowid
	/// 		}
	///
	/// 		func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
	/// 			_rowid = 1
	/// 		}
	///
	/// 		var eof: Bool {
	/// 			_rowid > 2147483647
	/// 		}
	/// 	}
	///
	/// 	required init(arguments: [String]) {
	/// 		// Arguments not used
	/// 	}
	///
	/// 	var declaration: String {
	/// 		"CREATE TABLE x(value)"
	/// 	}
	///
	/// 	var options: Database.VirtualTableModuleOptions {
	/// 		[.innocuous]
	/// 	}
	///
	/// 	func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
	/// 		.ok
	/// 	}
	///
	/// 	func openCursor() -> VirtualTableCursor {
	/// 		Cursor()
	/// 	}
	/// }
	/// ```
	///
	/// - parameter name: The name of the virtual table module
	/// - parameter type: The class implementing the virtual table module
	/// - parameter eponymousOnly: Whether the virtual table module is eponymous-only
	///
	/// - throws:  An error if the virtual table module can't be registered
	///
	/// - seealso: [Register A Virtual Table Implementation](https://www.sqlite.org/c3ref/create_module.html)
	/// - seealso: [The Virtual Table Mechanism Of SQLite](https://sqlite.org/vtab.html)
	public func addModule<T: VirtualTableModule>(_ name: String, type: T.Type, eponymousOnly: Bool = true) throws {
		// Flesh out the struct containing the virtual table functions used by SQLite
		var module_struct = sqlite3_module(iVersion: 0, xCreate: nil, xConnect: xConnect, xBestIndex: xBestIndex, xDisconnect: xDisconnect, xDestroy: nil,
		   xOpen: xOpen, xClose: xClose, xFilter: xFilter, xNext: xNext, xEof: xEof, xColumn: xColumn, xRowid: xRowid, xUpdate: nil, xBegin: nil, xSync: nil, xCommit: nil, xRollback: nil, xFindFunction: nil, xRename: nil, xSavepoint: nil, xRelease: nil, xRollbackTo: nil, xShadowName: nil)

		if !eponymousOnly {
			module_struct.xCreate = xCreate
			module_struct.xDestroy = xDestroy
		}

		// client_data must live until the xDestroy function is invoked; store it as a +1 object
		let client_data = VirtualTableModuleClientData(module: &module_struct) { args -> VirtualTableModule in
			return try T(arguments: args)
		}
		let client_data_ptr = Unmanaged.passRetained(client_data).toOpaque()

		guard sqlite3_create_module_v2(db, name, client_data.module, client_data_ptr, { client_data in
			// Balance the +1 retain above
			Unmanaged<VirtualTableModuleClientData>.fromOpaque(UnsafeRawPointer(client_data.unsafelyUnwrapped)).release()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding module \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}
}

// MARK: - Implementations

// xCreate() is the same functionally as xConnect() but must be a different pointer for non-eponymous tables
func xCreate(_ db: OpaquePointer?, _ pAux: UnsafeMutableRawPointer?, _ argc: Int32, _ argv: UnsafePointer<UnsafePointer<Int8>?>?, _ ppVTab:UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab>?>?, _ pzErr: UnsafeMutablePointer<UnsafeMutablePointer<Int8>?>?) -> Int32 {
	return xConnect(db, pAux, argc, argv, ppVTab, pzErr)
}

func xDestroy(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> Int32 {
	return xDisconnect(pVTab)
}

func xConnect(_ db: OpaquePointer?, _ pAux: UnsafeMutableRawPointer?, _ argc: Int32, _ argv: UnsafePointer<UnsafePointer<Int8>?>?, _ ppVTab: UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab>?>?, _ pzErr: UnsafeMutablePointer<UnsafeMutablePointer<Int8>?>?) -> Int32 {
	let args = UnsafeBufferPointer(start: argv, count: Int(argc))
	let arguments = args.map { String(utf8String: $0.unsafelyUnwrapped).unsafelyUnwrapped }

	let virtualTable: VirtualTableModule
	do {
		let clientData = Unmanaged<Database.VirtualTableModuleClientData>.fromOpaque(UnsafeRawPointer(pAux.unsafelyUnwrapped)).takeUnretainedValue()
		virtualTable = try clientData.construct(arguments)
	}

	catch let error as SQLiteError {
		os_log("Error connecting to virtual table module: %{public}@", type: .info, error.description)
		pzErr.unsafelyUnwrapped.pointee = feisty_db_sqlite3_strdup(error.message)
		return error.code.code
	}

	catch let error {
		os_log("Error connecting to virtual table module: %{public}@", type: .info, error.localizedDescription)
		pzErr.unsafelyUnwrapped.pointee = feisty_db_sqlite3_strdup(error.localizedDescription)
		return SQLITE_ERROR
	}

	let rc = sqlite3_declare_vtab(db, virtualTable.declaration)
	guard rc == SQLITE_OK else {
		return rc
	}

	let options = virtualTable.options
	feisty_db_sqlite3_vtab_config_constraint_support(db, options.contains(.constraintSupport) ? 1 : 0)
	if options.contains(.innocuous) {
		feisty_db_sqlite3_vtab_config_innocuous(db)
	}
	if options.contains(.directOnly) {
		feisty_db_sqlite3_vtab_config_directonly(db)
	}

	let vtab = sqlite3_malloc(Int32(MemoryLayout<feisty_db_sqlite3_vtab>.size))
	guard vtab != nil else {
		return SQLITE_NOMEM
	}

	// virtualTable must live until the xDisconnect function is invoked; store it as a +1 object in ptr
	let ptr = Unmanaged.passRetained(virtualTable as AnyObject).toOpaque()

	let vtab_ptr = vtab.unsafelyUnwrapped.bindMemory(to: feisty_db_sqlite3_vtab.self, capacity: 1)
	vtab_ptr.pointee.virtual_table_module_ptr = ptr
	vtab_ptr.withMemoryRebound(to: sqlite3_vtab.self, capacity: 1) {
		ppVTab.unsafelyUnwrapped.pointee = $0
	}

	return SQLITE_OK
}

func xDisconnect(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> Int32 {
	pVTab.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab.self, capacity: 1) { vtab in
		// Balance the +1 retain in xConnect()
		Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(vtab.pointee.virtual_table_module_ptr)).release()
	}
	sqlite3_free(pVTab)
	return SQLITE_OK
}

func xBestIndex(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ pIdxInfo: UnsafeMutablePointer<sqlite3_index_info>?) -> Int32  {
	return pVTab.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab.self, capacity: 1) { vtab -> Int32 in
		let virtualTable = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(vtab.pointee.virtual_table_module_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! VirtualTableModule
		do {
			let result = try virtualTable.bestIndex(&pIdxInfo.unsafelyUnwrapped.pointee)
			switch result {
			case .ok: 			return SQLITE_OK
			case .constraint: 	return SQLITE_CONSTRAINT
			}
		}

		catch let error as SQLiteError {
			os_log("Error in bestIndex(): %{public}@", type: .info, error.description)
			sqlite3_free(vtab.pointee.base.zErrMsg)
			vtab.pointee.base.zErrMsg = feisty_db_sqlite3_strdup(error.message)
			return error.code.code
		}

		catch let error {
			os_log("Error in bestIndex(): %{public}@", type: .info, error.localizedDescription)
			sqlite3_free(vtab.pointee.base.zErrMsg)
			vtab.pointee.base.zErrMsg = feisty_db_sqlite3_strdup(error.localizedDescription)
			return SQLITE_ERROR
		}
	}
}

func xOpen(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ ppCursor: UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab_cursor>?>?) -> Int32 {
	return pVTab.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab.self, capacity: 1) { vtab -> Int32 in
		let virtualTable = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(vtab.pointee.virtual_table_module_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! VirtualTableModule

		let cursor: VirtualTableCursor
		do {
			cursor = try virtualTable.openCursor()
		}

		catch let error {
			os_log("Error opening cursor: %{public}@", type: .info, error.localizedDescription)

			let description = error.localizedDescription
			let len = description.utf8.count + 1
			let mem = sqlite3_malloc(Int32(len))
			if mem != nil {
				let str = mem!.assumingMemoryBound(to: Int8.self)
				strncpy(str, description, len)
				vtab.pointee.base.zErrMsg = str
			}

			return SQLITE_ERROR
		}

		let curs = sqlite3_malloc(Int32(MemoryLayout<feisty_db_sqlite3_vtab_cursor>.size))
		guard curs != nil else {
			return SQLITE_NOMEM
		}

		// cursor must live until the xClose function is invoked; store it as a +1 object in ptr
		let ptr = Unmanaged.passRetained(cursor as AnyObject).toOpaque()

		let curs_ptr = curs.unsafelyUnwrapped.bindMemory(to: feisty_db_sqlite3_vtab_cursor.self, capacity: 1)
		curs_ptr.pointee.virtual_table_cursor_ptr = ptr
		curs_ptr.withMemoryRebound(to: sqlite3_vtab_cursor.self, capacity: 1) {
			ppCursor.unsafelyUnwrapped.pointee = $0
		}

		return SQLITE_OK
	}
}

func xClose(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?) -> Int32 {
	pCursor.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab_cursor.self, capacity: 1) { curs in
		// Balance the +1 retain above
		Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(curs.pointee.virtual_table_cursor_ptr)).release()
	}
	sqlite3_free(pCursor)
	return SQLITE_OK
}

func xFilter(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?, _ idxNum: Int32, _ idxStr: UnsafePointer<Int8>?, _ argc: Int32, _ argv: UnsafeMutablePointer<OpaquePointer?>?) -> Int32 {
	let args = UnsafeBufferPointer(start: argv, count: Int(argc))
	let arguments = args.map { DatabaseValue($0.unsafelyUnwrapped) }

	return pCursor.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab_cursor.self, capacity: 1) { curs -> Int32 in
		let cursor = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(curs.pointee.virtual_table_cursor_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! VirtualTableCursor
		var name: String? = nil
		if idxStr != nil {
			name = String(utf8String: idxStr.unsafelyUnwrapped).unsafelyUnwrapped
		}

		do {
			try cursor.filter(arguments, indexNumber: idxNum, indexName: name)
			return SQLITE_OK
		}

		catch let error as SQLiteError {
			os_log("Error in filter(): %{public}@", type: .info, error.description)
			sqlite3_free(curs.pointee.base.pVtab.pointee.zErrMsg)
			curs.pointee.base.pVtab.pointee.zErrMsg = feisty_db_sqlite3_strdup(error.message)
			return error.code.code
		}

		catch let error {
			os_log("Error in filter(): %{public}@", type: .info, error.localizedDescription)
			sqlite3_free(curs.pointee.base.pVtab.pointee.zErrMsg)
			curs.pointee.base.pVtab.pointee.zErrMsg = feisty_db_sqlite3_strdup(error.localizedDescription)
			return SQLITE_ERROR
		}
	}
}

func xNext(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?) -> Int32 {
	return pCursor.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab_cursor.self, capacity: 1) { curs -> Int32 in
		let cursor = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(curs.pointee.virtual_table_cursor_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! VirtualTableCursor
		do {
			try cursor.next()
			return SQLITE_OK
		}

		catch let error as SQLiteError {
			os_log("Error in next(): %{public}@", type: .info, error.description)
			sqlite3_free(curs.pointee.base.pVtab.pointee.zErrMsg)
			curs.pointee.base.pVtab.pointee.zErrMsg = feisty_db_sqlite3_strdup(error.message)
			return error.code.code
		}

		catch let error {
			os_log("Error in next(): %{public}@", type: .info, error.localizedDescription)
			sqlite3_free(curs.pointee.base.pVtab.pointee.zErrMsg)
			curs.pointee.base.pVtab.pointee.zErrMsg = feisty_db_sqlite3_strdup(error.localizedDescription)
			return SQLITE_ERROR
		}
	}
}

func xEof(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?) -> Int32 {
	return pCursor.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab_cursor.self, capacity: 1) { curs -> Int32 in
		let cursor = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(curs.pointee.virtual_table_cursor_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! VirtualTableCursor
		return cursor.eof ? 1 : 0
	}
}

func xColumn(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?, _ pCtx: OpaquePointer?, _ i: Int32) -> Int32 {
	return pCursor.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab_cursor.self, capacity: 1) { curs -> Int32 in
		let cursor = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(curs.pointee.virtual_table_cursor_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! VirtualTableCursor
		do {
			let value = try cursor.column(i)
			set_sqlite3_result(pCtx, value: value)
			return SQLITE_OK
		}

		catch let error as SQLiteError {
			os_log("Error in column(%i): %{public}@", type: .info, i, error.description)
			sqlite3_free(curs.pointee.base.pVtab.pointee.zErrMsg)
			curs.pointee.base.pVtab.pointee.zErrMsg = feisty_db_sqlite3_strdup(error.message)
			return error.code.code
		}

		catch let error {
			os_log("Error in column(%i): %{public}@", type: .info, i, error.localizedDescription)
			sqlite3_free(curs.pointee.base.pVtab.pointee.zErrMsg)
			curs.pointee.base.pVtab.pointee.zErrMsg = feisty_db_sqlite3_strdup(error.localizedDescription)
			return SQLITE_ERROR
		}
	}
}

func xRowid(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?, _ pRowid: UnsafeMutablePointer<sqlite3_int64>?) -> Int32 {
	return pCursor.unsafelyUnwrapped.withMemoryRebound(to: feisty_db_sqlite3_vtab_cursor.self, capacity: 1) { curs -> Int32 in
		let cursor = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(curs.pointee.virtual_table_cursor_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! VirtualTableCursor
		do {
			let rowid = try cursor.rowid()
			pRowid.unsafelyUnwrapped.pointee = rowid
			return SQLITE_OK
		}

		catch let error as SQLiteError {
			os_log("Error in rowid(): %{public}@", type: .info, error.description)
			sqlite3_free(curs.pointee.base.pVtab.pointee.zErrMsg)
			curs.pointee.base.pVtab.pointee.zErrMsg = feisty_db_sqlite3_strdup(error.message)
			return error.code.code
		}

		catch let error {
			os_log("Error in rowid(): %{public}@", type: .info, error.localizedDescription)
			sqlite3_free(curs.pointee.base.pVtab.pointee.zErrMsg)
			curs.pointee.base.pVtab.pointee.zErrMsg = feisty_db_sqlite3_strdup(error.localizedDescription)
			return SQLITE_ERROR
		}
	}
}
