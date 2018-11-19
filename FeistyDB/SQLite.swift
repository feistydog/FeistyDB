//
// Copyright (c) 2018 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation

/// SQLite library information.
public struct SQLite {
	/// The version of SQLite in the format *X.Y.Z*, for example `3.25.3`
	///
	/// - seealso: [Run-Time Library Version Numbers](https://www.sqlite.org/c3ref/libversion.html)
	static let version = String(cString: sqlite3_libversion())

	/// The version of SQLite in the format *(X\*1000000 + Y\*1000 + Z)*, such as `3025003`
	///
	/// - seealso: [Run-Time Library Version Numbers](https://www.sqlite.org/c3ref/libversion.html)
	static let versionNumber = Int(sqlite3_libversion_number())

	/// The identifier of the SQLite source tree, for example `89e099fbe5e13c33e683bef07361231ca525b88f7907be7092058007b75036f2`
	///
	/// - seealso: [Run-Time Library Version Numbers](https://www.sqlite.org/c3ref/libversion.html)
	static let sourceID = String(cString: sqlite3_sourceid())

	/// The number of bytes of memory `malloc`ed but not yet `free`d by SQLite
	static var memoryUsed: Int64 {
		return sqlite3_memory_used()
	}

	/// Returns the maximum amount of memory used by SQLite since the memory highwater mark was last reset.
	///
	/// - parameter reset: If `true` the memory highwater mark is reset to the value of `memoryUsed`
	static func memoryHighwater(reset: Bool = false) -> Int64 {
		return sqlite3_memory_highwater(reset ? 1 : 0)
	}

	/// The keywords understood by SQLite.
	///
	/// - note: Keywords in SQLite are not case sensitive.
	///
	/// - seealso: [SQL Keyword Checking](https://www.sqlite.org/c3ref/keyword_check.html)
	static let keywords: Set<String> = {
		var keywords = Set<String>()
		for i in 0 ..< sqlite3_keyword_count() {
			var chars: UnsafePointer<Int8>?
			var count = Int32(0)
			guard sqlite3_keyword_name(i, &chars, &count) == SQLITE_OK, chars != nil else {
				continue
			}

			let mutableChars = UnsafeMutablePointer(mutating: chars!)
			let data = Data(bytesNoCopy: mutableChars, count: Int(count), deallocator: .none)
			if let keyword = String(data: data, encoding: .utf8) {
				keywords.insert(keyword)
			}
		}
		return keywords
	}()

	/// Tests whether `identifier` is an SQLite keyword.
	///
	/// - parameter identifier: The string to check
	///
	/// - returns: `True` if `identifier` is an SQLite keyword, `False` otherwise
	///
	/// - seealso: [SQL Keyword Checking](https://www.sqlite.org/c3ref/keyword_check.html)
	static func isKeyword(_ identifier: String) -> Bool {
		return identifier.withCString {
			return sqlite3_keyword_check($0, Int32(strlen($0)))
		} != 0
	}

	/// Generates `count` bytes of randomness.
	///
	/// - parameter count: The number of random bytes to generate
	///
	/// - returns: A `Data` object containing `count` bytes of randomness
	///
	/// - seealso: [Pseudo-Random Number Generator](https://www.sqlite.org/c3ref/randomness.html)
	static func randomness(_ count: Int) -> Data {
		var data = Data(count: count)
		data.withUnsafeMutableBytes {
			sqlite3_randomness(Int32(count), $0)
		}
		return data
	}
}
