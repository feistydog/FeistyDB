//
// Copyright (c) 2019 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

// MARK: Int32
extension Statement {
	/// Binds the values in `array` to the SQL parameter at `index` using the sqlite3 Carray extension
	///
	/// When using the Carray extension in `FeistyDB` the number of array elements must be explicitly bound:
	/// ```
	/// let primes = [ 3, 5, 7 ]
	/// let statement = try db.prepare(sql: "SELECT * FROM numbers WHERE value IN carray(?1,?2,'int32');")
	///	try statement.bind(array: primes, toParameter: 1)
	/// try statement.bind(value: primes.count, toParameter: 2)
	/// ```
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - requires: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter array: An array of values to bind to the SQL parameter
	/// - parameter index: The index of the SQL parameter to bind
	///
	/// - throws: An error if `array` couldn't be bound
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<S: Collection>(array: S, toParameter index: Int) throws where S.Element == Int32 {
		let idx = Int32(index)

		let mem = UnsafeMutableBufferPointer<Int32>.allocate(capacity: array.count)
		_ = mem.initialize(from: array)

		try carray_pointer_type.utf8Start.withMemoryRebound(to: Int8.self, capacity: carray_pointer_type.utf8CodeUnitCount) { type in
			guard sqlite3_bind_pointer(stmt, idx, mem.baseAddress, type, {
				$0?.deallocate()
			}) == SQLITE_OK else {
				throw SQLiteError("Error binding carray (int32) to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}
		}
	}

	/// Binds the values in `array` to SQL parameter `name` using the sqlite3 Carray extension
	///
	/// When using the Carray extension in `FeistyDB` the number of array elements must be explicitly bound:
	/// ```
	/// let primes = [ 3, 5, 7 ]
	/// let statement = try db.prepare(sql: "SELECT * FROM numbers WHERE value IN carray(:primes,?2,'int32');")
	///	try statement.bind(array: primes, toParameter: ":primes")
	/// try statement.bind(value: primes.count, toParameter: 2)
	/// ```
	///
	/// - parameter array: An array of values to bind to the SQL parameter
	/// - parameter name: The name of the SQL parameter to bind
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `array` couldn't be bound
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<S: Collection>(array: S, toParameter name: String) throws where S.Element == Int32 {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		guard idx > 0 else {
			throw DatabaseError("Unknown parameter \"\(name)\"")
		}

		try bind(array: array, toParameter: Int(idx))
	}
}

// MARK: Int64
extension Statement {
	/// Binds the values in `array` to the SQL parameter at `index` using the sqlite3 Carray extension
	///
	/// When using the Carray extension in `FeistyDB` the number of array elements must be explicitly bound:
	/// ```
	/// let primes = [ 87178291199, 99194853094755497 ]
	/// let statement = try db.prepare(sql: "SELECT * FROM numbers WHERE value IN carray(?1,?2,'int64');")
	///	try statement.bind(array: primes, toParameter: 1)
	/// try statement.bind(value: primes.count, toParameter: 2)
	/// ```
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - requires: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter array: An array of values to bind to the SQL parameter
	/// - parameter index: The index of the SQL parameter to bind
	///
	/// - throws: An error if `array` couldn't be bound
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<S: Collection>(array: S, toParameter index: Int) throws where S.Element == Int64 {
		let idx = Int32(index)

		let mem = UnsafeMutableBufferPointer<Int64>.allocate(capacity: array.count)
		_ = mem.initialize(from: array)

		try carray_pointer_type.utf8Start.withMemoryRebound(to: Int8.self, capacity: carray_pointer_type.utf8CodeUnitCount) { type in
			guard sqlite3_bind_pointer(stmt, idx, mem.baseAddress, type, {
				$0?.deallocate()
			}) == SQLITE_OK else {
				throw SQLiteError("Error binding carray (int64) to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}
		}
	}

	/// Binds the values in `array` to SQL parameter `name` using the sqlite3 Carray extension
	///
	/// When using the Carray extension in `FeistyDB` the number of array elements must be explicitly bound:
	/// ```
	/// let primes = [ 87178291199, 99194853094755497 ]
	/// let statement = try db.prepare(sql: "SELECT * FROM numbers WHERE value IN carray(:primes,?2,'int64');")
	///	try statement.bind(array: primes, toParameter: ":primes")
	/// try statement.bind(value: primes.count, toParameter: 2)
	/// ```
	///
	/// - parameter array: An array of values to bind to the SQL parameter
	/// - parameter name: The name of the SQL parameter to bind
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `array` couldn't be bound
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<S: Collection>(array: S, toParameter name: String) throws where S.Element == Int64 {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		guard idx > 0 else {
			throw DatabaseError("Unknown parameter \"\(name)\"")
		}

		try bind(array: array, toParameter: Int(idx))
	}
}

// MARK: - Double
extension Statement {
	/// Binds the values in `array` to the SQL parameter at `index` using the sqlite3 Carray extension
	///
	/// When using the Carray extension in `FeistyDB` the number of array elements must be explicitly bound:
	/// ```
	/// let specials = [ Double.pi, Double.nan, Double.infinity ]
	/// let statement = try db.prepare(sql: "SELECT * FROM numbers WHERE value IN carray(?1,?2,'double');")
	///	try statement.bind(array: specials, toParameter: 1)
	/// try statement.bind(value: specials.count, toParameter: 2)
	/// ```
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - requires: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter array: An array of values to bind to the SQL parameter
	/// - parameter index: The index of the SQL parameter to bind
	///
	/// - throws: An error if `array` couldn't be bound
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<S: Collection>(array: S, toParameter index: Int) throws where S.Element == Double {
		let idx = Int32(index)

		let mem = UnsafeMutableBufferPointer<Double>.allocate(capacity: array.count)
		_ = mem.initialize(from: array)

		try carray_pointer_type.utf8Start.withMemoryRebound(to: Int8.self, capacity: carray_pointer_type.utf8CodeUnitCount) { type in
			guard sqlite3_bind_pointer(stmt, idx, mem.baseAddress, type, {
				$0?.deallocate()
			}) == SQLITE_OK else {
				throw SQLiteError("Error binding carray (double) to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}
		}
	}

	/// Binds the values in `array` to SQL parameter `name` using the sqlite3 Carray extension
	///
	/// When using the Carray extension in `FeistyDB` the number of array elements must be explicitly bound:
	/// ```
	/// let specials = [ Double.pi, Double.nan, Double.infinity ]
	/// let statement = try db.prepare(sql: "SELECT * FROM numbers WHERE value IN carray(:specials,?2,'double');")
	///	try statement.bind(array: specials, toParameter: ":specials")
	/// try statement.bind(value: specials.count, toParameter: 2)
	/// ```
	///
	/// - parameter array: An array of values to bind to the SQL parameter
	/// - parameter name: The name of the SQL parameter to bind
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `array` couldn't be bound
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<S: Collection>(array: S, toParameter name: String) throws where S.Element == Double {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		guard idx > 0 else {
			throw DatabaseError("Unknown parameter \"\(name)\"")
		}

		try bind(array: array, toParameter: Int(idx))
	}
}

// MARK: - String
extension Statement {
	/// Binds the values in `array` to the SQL parameter at `index` using the sqlite3 Carray extension
	///
	/// When using the Carray extension in `FeistyDB` the number of array elements must be explicitly bound:
	/// ```
	/// let pets = [ "dog", "dragon", "hedgehog" ]
	/// let statement = try db.prepare(sql: "SELECT * FROM animals WHERE kind IN carray(?1,?2,'char*');")
	///	try statement.bind(array: pets, toParameter: 1)
	/// try statement.bind(value: pets.count, toParameter: 2)
	/// ```
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - requires: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter array: An array of values to bind to the SQL parameter
	/// - parameter index: The index of the SQL parameter to bind
	///
	/// - throws: An error if `array` couldn't be bound
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<S: Collection>(array: S, toParameter index: Int) throws where S.Element == String {
		let idx = Int32(index)

		let count = array.count

		let utf8_character_counts = array.map { $0.utf8.count + 1 }
		let utf8_offsets = [ 0 ] + scan(utf8_character_counts, 0, +)
		let utf8_buf_size = utf8_offsets.last!

		let ptr_size = MemoryLayout<UnsafePointer<Int8>>.stride * count
		let alloc_size = ptr_size + utf8_buf_size

		let mem = UnsafeMutableRawPointer.allocate(byteCount: alloc_size, alignment: MemoryLayout<UnsafePointer<Int8>>.alignment)

		let ptrs = mem.bindMemory(to: UnsafeMutablePointer<Int8>.self, capacity: count)
		let utf8 = (mem + ptr_size).bindMemory(to: Int8.self, capacity: utf8_buf_size)

		for(i, s) in array.enumerated() {
			let pos = utf8 + utf8_offsets[i]
			ptrs[i] = pos
			memcpy(pos, s, utf8_offsets[i + 1] - utf8_offsets[i])
		}

		try carray_pointer_type.utf8Start.withMemoryRebound(to: Int8.self, capacity: carray_pointer_type.utf8CodeUnitCount) { type in
			guard sqlite3_bind_pointer(stmt, idx, mem, type, {
				$0?.deallocate()
			}) == SQLITE_OK else {
				throw SQLiteError("Error binding carray (char*) to parameter \(idx)", takingDescriptionFromStatement: stmt)
			}
		}
	}

	/// Binds the values in `array` to SQL parameter `name` using the sqlite3 Carray extension
	///
	/// When using the Carray extension in `FeistyDB` the number of array elements must be explicitly bound:
	/// ```
	/// let pets = [ "dog", "dragon", "hedgehog" ]
	/// let statement = try db.prepare(sql: "SELECT * FROM animals WHERE kind IN carray(:pets,?2,'char*');")
	///	try statement.bind(array: pets, toParameter: ":pets")
	/// try statement.bind(value: pets.count, toParameter: 2)
	/// ```
	///
	/// - parameter array: An array of values to bind to the SQL parameter
	/// - parameter name: The name of the SQL parameter to bind
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `array` couldn't be bound
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<S: Collection>(array: S, toParameter name: String) throws where S.Element == String {
		let idx = sqlite3_bind_parameter_index(stmt, name)
		guard idx > 0 else {
			throw DatabaseError("Unknown parameter \"\(name)\"")
		}

		try bind(array: array, toParameter: Int(idx))
	}
}

// MARK: - Internals

/// Computes the accumulated result  of `seq`
private func accumulate<S: Sequence, U>(_ seq: S, _ initial: U, _ combine: (U, S.Element) -> U) -> [U] {
	var result: [U] = []
	result.reserveCapacity(seq.underestimatedCount)
	var runningResult = initial
	for element in seq {
		runningResult = combine(runningResult, element)
		result.append(runningResult)
	}
	return result
}

/// Computes the prefix sum of `seq`.
private func scan<S: Sequence, U>(_ seq: S, _ initial: U, _ combine: (U, S.Element) -> U) -> [U] {
	var result: [U] = []
	result.reserveCapacity(seq.underestimatedCount)
	var runningResult = initial
	for element in seq {
		runningResult = combine(runningResult, element)
		result.append(runningResult)
	}
	return result
}

// SQLite pointer types are static strings
private let carray_pointer_type: StaticString = "carray"
