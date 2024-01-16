//
// Copyright (c) 2015 - 2024 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation
import CSQLite

extension Database {
	/// A custom SQL function.
	///
	/// - parameter values: The SQL function parameters
	///
	/// - throws: `Error`
	///
	/// - returns: The result of applying the function to `values`
	public typealias SQLFunction = (_ values: [DatabaseValue]) throws -> DatabaseValue

	/// Custom SQL function flags
	///
	/// - seealso: [Function Flags](https://www.sqlite.org/c3ref/c_deterministic.html)
	public struct SQLFunctionFlags: OptionSet {
		public let rawValue: Int

		public init(rawValue: Int) {
			self.rawValue = rawValue
		}

		/// The function gives the same output when the input parameters are the same
		public static let deterministic = SQLFunctionFlags(rawValue: 1 << 0)
		/// The function may only be invoked from top-level SQL, and cannot be used in views or triggers
		/// nor in schema structures such as `CHECK` constraints, `DEFAULT` clauses, expression indexes, partial indexes, or generated columns
		public static let directOnly = SQLFunctionFlags(rawValue: 1 << 1)
		/// Indicates to SQLite that a function may call `sqlite3_value_subtype()` to inspect the sub-types of its arguments
		public static let subtype = SQLFunctionFlags(rawValue: 1 << 2)
		/// The function is unlikely to cause problems even if misused.
		/// An innocuous function should have no side effects and should not depend on any values other than its input parameters.
		public static let innocuous = SQLFunctionFlags(rawValue: 1 << 3)
		/// Indicates to SQLite that a function may call `sqlite3_result_subtype()` to to cause a sub-type to be associated with its result.
		public static let resultSubtype = SQLFunctionFlags(rawValue: 1 << 4)
	}
}

extension Database.SQLFunctionFlags {
	/// Returns the value of `self` using SQLite's flag values
	func asSQLiteFlags() -> Int32 {
		var flags: Int32 = 0

		if contains(.deterministic) {
			flags |= SQLITE_DETERMINISTIC
		}
		if contains(.directOnly) {
			flags |= SQLITE_DIRECTONLY
		}
		if contains(.subtype) {
			flags |= SQLITE_SUBTYPE
		}
		if contains(.innocuous) {
			flags |= SQLITE_INNOCUOUS
		}
		if contains(.resultSubtype) {
			flags |= SQLITE_RESULT_SUBTYPE
		}

		return flags
	}
}

/// A custom SQL aggregate function.
public protocol SQLAggregateFunction {
	/// Invokes the aggregate function for one or more values in a row.
	///
	/// - parameter values: The SQL function parameters
	///
	/// - throws: `Error`
	func step(_ values: [DatabaseValue]) throws

	/// Returns the current value of the aggregate function.
	///
	/// - note: This should also reset any function context to defaults.
	///
	/// - throws: `Error`
	///
	/// - returns: The current value of the aggregate function.
	func final() throws -> DatabaseValue
}

/// A custom SQL aggregate window function.
public protocol SQLAggregateWindowFunction: SQLAggregateFunction {
	/// Invokes the inverse aggregate function for one or more values in a row.
	///
	/// - parameter values: The SQL function parameters
	///
	/// - throws: `Error`
	func inverse(_ values: [DatabaseValue]) throws

	/// Returns the current value of the aggregate window function.
	///
	/// - throws: `Error`
	///
	/// - returns: The current value of the aggregate window function.
	func value() throws -> DatabaseValue
}

extension Database {
	/// Adds a custom SQL scalar function.
	///
	/// For example, a localized uppercase scalar function could be implemented as:
	/// ```swift
	/// try db.addFunction("localizedUppercase", arity: 1) { values in
	///     let value = values.first.unsafelyUnwrapped
	///     switch value {
	///     case .text(let s):
	///         return .text(s.localizedUppercase())
	///     default:
	///         return value
	///     }
	/// }
	/// ```
	///
	/// - parameter name: The name of the function
	/// - parameter arity: The number of arguments the function accepts
	/// - parameter flags: Flags affecting the function's use by SQLite
	/// - parameter block: A closure that returns the result of applying the function to the supplied arguments
	///
	/// - throws: An error if the SQL scalar function couldn't be added
	///
	/// - seealso: [Create Or Redefine SQL Functions](https://sqlite.org/c3ref/create_function.html)
	public func addFunction(_ name: String, arity: Int = -1, flags: SQLFunctionFlags = [.deterministic, .directOnly], _ block: @escaping SQLFunction) throws {
		let function_ptr = UnsafeMutablePointer<SQLFunction>.allocate(capacity: 1)
		function_ptr.initialize(to: block)

		let function_flags = SQLITE_UTF8 | flags.asSQLiteFlags()
		guard sqlite3_create_function_v2(db, name, Int32(arity), function_flags, function_ptr, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLFunction.self)

			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { DatabaseValue($0.unsafelyUnwrapped) }

			do {
				set_sqlite3_result(sqlite_context, value: try function_ptr.pointee(arguments))
			}

			catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, nil, nil, { context in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLFunction.self)
			function_ptr.deinitialize(count: 1)
			function_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding SQL scalar function \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}

	/// Adds a custom SQL aggregate function.
	///
	/// For example, an integer sum aggregate function could be implemented as:
	/// ```swift
	/// class IntegerSumAggregateFunction: SQLAggregateFunction {
	///     func step(_ values: [DatabaseValue]) throws {
	///         let value = values.first.unsafelyUnwrapped
	///         switch value {
	///             case .integer(let i):
	///                 sum += i
	///             default:
	///                 throw DatabaseError("Only integer values supported")
	///         }
	///     }
	///
	///     func final() throws -> DatabaseValue {
	///         defer {
	///             sum = 0
	///         }
	///         return DatabaseValue(sum)
	///     }
	///
	///     var sum: Int64 = 0
	/// }
	/// ```
	///
	/// - parameter name: The name of the aggregate function
	/// - parameter arity: The number of arguments the function accepts
	/// - parameter flags: Flags affecting the function's use by SQLite
	/// - parameter aggregateFunction: An object defining the aggregate function
	///
	/// - throws: An error if the SQL aggregate function can't be added
	///
	/// - seealso: [Create Or Redefine SQL Functions](https://sqlite.org/c3ref/create_function.html)
	public func addAggregateFunction(_ name: String, arity: Int = -1, flags: SQLFunctionFlags = [.deterministic, .directOnly], _ function: SQLAggregateFunction) throws {
		// function must live until the xDelete function is invoked
		let context_ptr = UnsafeMutablePointer<SQLAggregateFunction>.allocate(capacity: 1)
		context_ptr.initialize(to: function)

		let function_flags = SQLITE_UTF8 | flags.asSQLiteFlags()
		guard sqlite3_create_function_v2(db, name, Int32(arity), function_flags, context_ptr, nil, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateFunction.self)
			let function = context_ptr.pointee

			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { DatabaseValue($0.unsafelyUnwrapped) }

			do {
				try function.step(arguments)
			}

			catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { sqlite_context in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateFunction.self)
			let function = context_ptr.pointee

			do {
				set_sqlite3_result(sqlite_context, value: try function.final())
			}

			catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { context in
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateFunction.self)
			context_ptr.deinitialize(count: 1)
			context_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding SQL aggregate function \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}

	/// Adds a custom SQL aggregate window function.
	///
	/// For example, an integer sum aggregate window function could be implemented as:
	/// ```swift
	/// class IntegerSumAggregateWindowFunction: SQLAggregateWindowFunction {
	///     func step(_ values: [DatabaseValue]) throws {
	///         let value = values.first.unsafelyUnwrapped
	///         switch value {
	///             case .integer(let i):
	///                 sum += i
	///             default:
	///                 throw DatabaseError("Only integer values supported")
	///         }
	///     }
	///
	///     func inverse(_ values: [DatabaseValue]) throws {
	///         let value = values.first.unsafelyUnwrapped
	///         switch value {
	///             case .integer(let i):
	///                 sum -= i
	///             default:
	///                 throw DatabaseError("Only integer values supported")
	///         }
	///     }
	///
	///     func value() throws -> DatabaseValue {
	///         return DatabaseValue(sum)
	///     }
	///
	///     func final() throws -> DatabaseValue {
	///         defer {
	///             sum = 0
	///         }
	///         return DatabaseValue(sum)
	///     }
	///
	///     var sum: Int64 = 0
	/// }
	/// ```
	///
	/// - parameter name: The name of the aggregate window function
	/// - parameter arity: The number of arguments the function accepts
	/// - parameter flags: Flags affecting the function's use by SQLite
	/// - parameter aggregateWindowFunction: An object defining the aggregate window function
	///
	/// - throws: An error if the SQL aggregate window function can't be added
	///
	/// - seealso: [User-Defined Aggregate Window Functions](https://sqlite.org/windowfunctions.html#udfwinfunc)
	public func addAggregateWindowFunction(_ name: String, arity: Int = -1, flags: SQLFunctionFlags = [.deterministic, .directOnly], _ function: SQLAggregateWindowFunction) throws {
		let context_ptr = UnsafeMutablePointer<SQLAggregateWindowFunction>.allocate(capacity: 1)
		context_ptr.initialize(to: function)

		let function_flags = SQLITE_UTF8 | flags.asSQLiteFlags()
		guard sqlite3_create_window_function(db, name, Int32(arity), function_flags, context_ptr, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			let function = context_ptr.pointee

			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { DatabaseValue($0.unsafelyUnwrapped) }

			do {
				try function.step(arguments)
			}

			catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { sqlite_context in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			let function = context_ptr.pointee

			do {
				set_sqlite3_result(sqlite_context, value: try function.final())
			}

			catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { sqlite_context in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			let function = context_ptr.pointee

			do {
				set_sqlite3_result(sqlite_context, value: try function.value())
			}

			catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			let function = context_ptr.pointee

			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { DatabaseValue($0.unsafelyUnwrapped) }

			do {
				try function.inverse(arguments)
			}

			catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { context in
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			context_ptr.deinitialize(count: 1)
			context_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding SQL aggregate window function \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}

	/// Removes a custom SQL scalar, aggregate, or window function.
	///
	/// - parameter name: The name of the custom SQL function
	/// - parameter arity: The number of arguments the custom SQL function accepts
	///
	/// - throws: An error if the SQL function couldn't be removed
	public func removeFunction(_ name: String, arity: Int = -1) throws {
		guard sqlite3_create_function_v2(db, name, Int32(arity), SQLITE_UTF8, nil, nil, nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error removing SQL function \"\(name)\"", takingDescriptionFromDatabase: db)
		}
	}
}

/// Passes `value` to the appropriate `sqlite3_result` function
///
/// - parameter sqlite_context: An `sqlite3_context *` object
/// - parameter value: The value to pass
func set_sqlite3_result(_ sqlite_context: OpaquePointer!, value: DatabaseValue) {
	switch value {
	case .integer(let i):
		sqlite3_result_int64(sqlite_context, i)
	case .float(let f):
		sqlite3_result_double(sqlite_context, f)
	case .text(let t):
		sqlite3_result_text(sqlite_context, t, -1, SQLITE_TRANSIENT)
	case .blob(let b):
		b.withUnsafeBytes { bytes in
			sqlite3_result_blob(sqlite_context, bytes.baseAddress, Int32(b.count), SQLITE_TRANSIENT)
		}
	case .null:
		sqlite3_result_null(sqlite_context)
	}
}
