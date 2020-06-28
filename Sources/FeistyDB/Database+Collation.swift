//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation
import CSQLite

extension Database {
	/// A comparator for `String` objects.
	///
	/// - parameter lhs: The left-hand operand
	/// - parameter rhs: The right-hand operand
	///
	/// - returns: The result of comparing `lhs` to `rhs`
	public typealias StringComparator = (_ lhs: String, _ rhs: String) -> ComparisonResult

	/// Adds a custom collation function.
	///
	/// ```swift
	/// try db.addCollation("localizedCompare", { (lhs, rhs) -> ComparisonResult in
	///     return lhs.localizedCompare(rhs)
	/// })
	/// ```
	///
	/// - parameter name: The name of the custom collation sequence
	/// - parameter block: A string comparison function
	///
	/// - throws: An error if the collation function couldn't be added
	public func addCollation(_ name: String, _ block: @escaping StringComparator) throws {
		let function_ptr = UnsafeMutablePointer<StringComparator>.allocate(capacity: 1)
		function_ptr.initialize(to: block)
		guard sqlite3_create_collation_v2(db, name, SQLITE_UTF8, function_ptr, { (context, lhs_len, lhs_data, rhs_len, rhs_data) -> Int32 in
			// Have total faith that SQLite will pass valid parameters and use unsafelyUnwrapped
			let lhs = String(bytesNoCopy: UnsafeMutableRawPointer(mutating: lhs_data.unsafelyUnwrapped), length: Int(lhs_len), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
			let rhs = String(bytesNoCopy: UnsafeMutableRawPointer(mutating: rhs_data.unsafelyUnwrapped), length: Int(rhs_len), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped

			// Cast context to the appropriate type and call the comparator
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: StringComparator.self)
			let result = function_ptr.pointee(lhs, rhs)
			return Int32(result.rawValue)
		}, { context in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: StringComparator.self)
			function_ptr.deinitialize(count: 1)
			function_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding collation sequence \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}

	/// Removes a custom collation function.
	///
	/// - parameter name: The name of the custom collation sequence
	///
	/// - throws: An error if the collation function couldn't be removed
	public func removeCollation(_ name: String) throws {
		guard sqlite3_create_collation_v2(db, name, SQLITE_UTF8, nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error removing collation sequence \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}
}
