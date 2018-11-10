//
// Copyright (c) 2015 - 2018 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import XCTest
@testable import FeistyDB

extension DatabaseValue {
	init(_ i: Int64) {
		self = .integer(i)
	}

	init(_ t: String) {
		self = .text(t)
	}
}

class FeistyDBTests: XCTestCase {

    override func setUp() {
        super.setUp()
    }
    
    override func tearDown() {
        super.tearDown()
    }

	func testSQLiteKeywords() {
		XCTAssertTrue(SQLite.isKeyword("BEGIN"))
		XCTAssertTrue(SQLite.isKeyword("begin"))
		XCTAssertTrue(SQLite.isKeyword("BeGiN"))
		XCTAssertFalse(SQLite.isKeyword("BEGINNING"))
	}

	func testDatabase() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b);")

		let rowCount = 10

		for _ in 0..<rowCount {
			try! db.execute(sql: "insert into t1 default values;")
		}

		let count: Int = try! db.prepare(sql: "select count(*) from t1;").front()

		XCTAssertEqual(count, rowCount)
	}

	func testInsert() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a text);")

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [1])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["feisty"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [2.5])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [Data(count: 8)])

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [URL(fileURLWithPath: "/tmp")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [UUID()])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [Date()])

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [NSNull()])
	}

	func testIteration() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a);")

		let rowCount = 10

		for i in 0..<rowCount {
			try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [i]).execute()
		}

		let s = try! db.prepare(sql: "select * from t1;")
		var count = 0

		for row in s {
			for _ in row {
				XCTAssert(try! row.leftmostValue() as Int == count)
			}
			count += 1
		}

		XCTAssertEqual(count, rowCount)
	}

	func testIteration2() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a,b,c,d);")

		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind(parameterValues: [1,2,3,4]).execute()
		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind(parameterValues: ["a","b","c","d"]).execute()
		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind(parameterValues: ["a",2,"c",4]).execute()

		do {
			let s = try! db.prepare(sql: "select * from t1 limit 1 offset 0;")
			let r = try! s.firstRow()!
			let v = [DatabaseValue](r)

			XCTAssertEqual(v, [DatabaseValue(1),DatabaseValue(2),DatabaseValue(3),DatabaseValue(4)])
		}

		do {
			let s = try! db.prepare(sql: "select * from t1 limit 1 offset 1;")
			let r = try! s.firstRow()!
			let v = [DatabaseValue](r)

			XCTAssertEqual(v, [DatabaseValue("a"),DatabaseValue("b"),DatabaseValue("c"),DatabaseValue("d")])
		}
	}

	func testUUIDExtension() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(u text default (uuid4()));")
		try! db.execute(sql: "insert into t1 default values;")

		let u: UUID? = try! db.prepare(sql: "select * from t1 limit 1;").front()

		XCTAssertNotNil(u)
	}

	func testSHAExtension() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a);")
		try! db.execute(sql: "insert into t1 (a) values (sha256('lu'));")

		let s: String? = try! db.prepare(sql: "select hex(a) from t1 limit 1;").front()

		XCTAssertEqual(s, "80C0FCBBFA9D03D861B22230E67C380AFB545C12DE43094F3985128625858361")
	}

	func testEncodable() {
		let db = try! Database()

		struct TestStruct : ColumnConvertible, ParameterBindable, Codable {
			let a: Int
			let b: Float
			let c: Date
			let d: String
		}

		try! db.execute(sql: "create table t1(a);")

		let a = TestStruct(a: 1, b: 3.14, c: Date(), d: "Lu")

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [a])

		let b: TestStruct = try! db.prepare(sql: "select * from t1 limit 1;").front()

		XCTAssertEqual(a.a, b.a)
		XCTAssertEqual(a.c, b.c)
		XCTAssertEqual(a.d, b.d)
	}

	func testCustomCollation() {
		let db = try! Database()

		try! db.addCollation("reversed", { (a, b) -> ComparisonResult in
			return b.compare(a)
		})

		try! db.execute(sql: "create table t1(a text);")

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["a"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["c"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["z"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["e"])

		var str = ""
		let s = try! db.prepare(sql: "select * from t1 order by a collate reversed;")
		try! s.results { row in
			let c: String = try row.value(at: 0)
			str.append(c)
		}

		XCTAssertEqual(str, "zeca")
	}

	func testCustomFunction() {
		let db = try! Database()

		let rot13key: [Character: Character] = [
			"A": "N", "B": "O", "C": "P", "D": "Q", "E": "R", "F": "S", "G": "T", "H": "U", "I": "V", "J": "W", "K": "X", "L": "Y", "M": "Z",
			"N": "A", "O": "B", "P": "C", "Q": "D", "R": "E", "S": "F", "T": "G", "U": "H", "V": "I", "W": "J", "X": "K", "Y": "L", "Z": "M",
			"a": "n", "b": "o", "c": "p", "d": "q", "e": "r", "f": "s", "g": "t", "h": "u", "i": "v", "j": "w", "k": "x", "l": "y", "m": "z",
			"n": "a", "o": "b", "p": "c", "q": "d", "r": "e", "s": "f", "t": "g", "u": "h", "v": "i", "w": "j", "x": "k", "y": "l", "z": "m"]

		func rot13(_ s: String) -> String {
			return String(s.map { rot13key[$0] ?? $0 })
		}

		try! db.addFunction("rot13", arity: 1) { values in
			let value = values.first.unsafelyUnwrapped
			switch value {
			case .text(let s):
				return .text(rot13(s))
			default:
				return value
			}
		}

		try! db.execute(sql: "create table t1(a);")

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["this"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["is"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["only"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["a"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: ["test"])

		let s = try! db.prepare(sql: "select rot13(a) from t1;")
		let results = s.map { try! $0.leftmostValue() as String }

		XCTAssertEqual(results, ["guvf", "vf", "bayl", "n", "grfg"])

		try! db.removeFunction("rot13", arity: 1)
		XCTAssertThrowsError(try db.prepare(sql: "select rot13(a) from t1;"))
	}

	func testCustomTokenizer() {

		/// A word tokenizer using CFStringTokenizer
		class WordTokenizer: FTS5Tokenizer {
			var tokenizer: CFStringTokenizer!
			var text: CFString!

			required init(arguments: [String]) {
			}

			func setText(_ text: String, reason: Database.FTS5TokenizationReason) {
				self.text = text as CFString
				tokenizer = CFStringTokenizerCreate(kCFAllocatorDefault, self.text, CFRangeMake(0, CFStringGetLength(self.text)), kCFStringTokenizerUnitWord, nil)
			}

			func advance() -> Bool {
				let nextToken = CFStringTokenizerAdvanceToNextToken(tokenizer)
				guard nextToken != CFStringTokenizerTokenType(rawValue: 0) else {
					return false
				}
				return true
			}

			func currentToken() -> String? {
				let tokenRange = CFStringTokenizerGetCurrentTokenRange(tokenizer)
				guard tokenRange.location != kCFNotFound /*|| tokenRange.length != 0*/ else {
					return nil
				}
				return CFStringCreateWithSubstring(kCFAllocatorDefault, text, tokenRange) as String
			}

			func copyCurrentToken(to buffer: UnsafeMutablePointer<UInt8>, capacity: Int) throws -> Int {
				let tokenRange = CFStringTokenizerGetCurrentTokenRange(tokenizer)
				var bytesConverted = 0
				let charsConverted = CFStringGetBytes(text, tokenRange, CFStringBuiltInEncodings.UTF8.rawValue, 0, false, buffer, capacity, &bytesConverted)
				guard charsConverted > 0 else {
					throw DatabaseError("Insufficient buffer size")
				}
				return bytesConverted
			}
		}

		let db = try! Database()

		try! db.addTokenizer("word", type: WordTokenizer.self)

		try! db.execute(sql: "create virtual table t1 USING fts5(a, tokenize = 'word');")

		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["quick brown"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["fox"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["jumps over"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["the lazy dog"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["🦊🐶"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [""]).execute()
		try! db.prepare(sql: "insert into t1(a) values (NULL);").execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["quick"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["brown fox"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["jumps over the"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: ["lazy dog"]).execute()

		let s = try! db.prepare(sql: "select count(*) from t1 where t1 match 'o*';")
		let count: Int = try! s.front()
		XCTAssertEqual(count, 2)

		let statement = try! db.prepare(sql: "select * from t1 where t1 match 'o*';")
		try! statement.results { row in
			let s: String = try row.value(at: 0)
			XCTAssert(s.starts(with: "jumps over"))
		}
	}
	
	func testDatabaseBindings() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b);")

		for i in 0..<10 {
			try! db.prepare(sql: "insert into t1(a, b) values (?, ?);").bind(parameterValues: [i, nil]).execute()
		}

		let statement = try! db.prepare(sql: "select * from t1 where a = ?")
		try! statement.bind(value: 5, toParameter: 1)

		try! statement.results { row in
			let x: Int = try row.value(at: 0)
			let y: Int? = try row.value(named: "b")

			XCTAssertEqual(x, 5)
			XCTAssertEqual(y, nil)
		}
	}

	func testDatabaseNamedBindings() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b);")

		for i in 0..<10 {
			try! db.execute(sql: "insert into t1(a, b) values (:b, :a);", parameters: [":a": nil, ":b": i])
		}

		let statement = try! db.prepare(sql: "select * from t1 where a = :a")
		try! statement.bind(value: 5, toParameter: ":a")

		try! statement.results { row in
			let x: Int = try row.value(at: 0)
			let y: Int? = try row.value(at: 1)

			XCTAssertEqual(x, 5)
			XCTAssertEqual(y, nil)
		}
	}


	func testDatabaseQueue() {
	}

	func testConcurrentDatabaseQueue() {
	}

    func testSQLiteInsertPerformance() {
        self.measure {
			var db: OpaquePointer?
			sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)

			sqlite3_exec(db, "create table t1(a, b);", nil, nil, nil)

			let rowCount = 50_000
			for i in 0..<rowCount {
				var stmt: OpaquePointer?
				sqlite3_prepare_v2(db, "insert into t1(a, b) values (?, ?);", -1, &stmt, nil)

				sqlite3_bind_int64(stmt, 1, sqlite3_int64(i*2))
				sqlite3_bind_int64(stmt, 2, sqlite3_int64(i*2+1))

				sqlite3_step(stmt)
				sqlite3_finalize(stmt)
			}

			var stmt: OpaquePointer?
			sqlite3_prepare_v2(db, "select count(*) from t1;", -1, &stmt, nil)
			sqlite3_step(stmt)
			let count = Int(sqlite3_column_int64(stmt, 0))

			sqlite3_finalize(stmt)
			sqlite3_close(db)

			XCTAssertEqual(count, rowCount)
        }
    }

	func testDatabaseInsertPerformance() {
		self.measure {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			let rowCount = 50_000
			for i in 0..<rowCount {
				let s = try! db.prepare(sql: "insert into t1(a, b) values (?, ?);")

				try! s.bind(value: i*2, toParameter: 1)
				try! s.bind(value: i*2+1, toParameter: 2)

				try! s.execute()
			}

			let s = try! db.prepare(sql: "select count(*) from t1;")
			var count = 0
			try! s.results { row in
				count = try row.value(at: 0)
			}

			XCTAssertEqual(count, rowCount)
		}
	}

	func testSQLiteInsertPerformance2() {
		self.measure {
			var db: OpaquePointer?
			sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)

			sqlite3_exec(db, "create table t1(a, b);", nil, nil, nil)

			var stmt: OpaquePointer?
			sqlite3_prepare_v2(db, "insert into t1(a, b) values (?, ?);", -1, &stmt, nil)

			let rowCount = 50_000
			for i in 0..<rowCount {
				sqlite3_bind_int64(stmt, 1, sqlite3_int64(i*2))
				sqlite3_bind_int64(stmt, 2, sqlite3_int64(i*2+1))

				sqlite3_step(stmt)

				sqlite3_clear_bindings(stmt)
				sqlite3_reset(stmt)
			}

			sqlite3_finalize(stmt)

			sqlite3_prepare_v2(db, "select count(*) from t1;", -1, &stmt, nil)
			sqlite3_step(stmt)
			let count = Int(sqlite3_column_int64(stmt, 0))

			sqlite3_finalize(stmt)
			sqlite3_close(db)

			XCTAssertEqual(count, rowCount)
		}
	}

	func testDatabaseInsertPerformance2() {
		self.measure {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			var s = try! db.prepare(sql: "insert into t1(a, b) values (?, ?);")

			let rowCount = 50_000
			for i in 0..<rowCount {
				try! s.bind(value: i*2, toParameter: 1)
				try! s.bind(value: i*2+1, toParameter: 2)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count: Int = try! s.front()

			XCTAssertEqual(count, rowCount)
		}
	}

	func testDatabaseInsertPerformance31() {
		self.measure {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);")

			var s = try! db.prepare(sql: "insert into t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

			let values: [Int?] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]

			let rowCount = 10_000
			for _ in 0..<rowCount {
				try! s.bind(parameterValues: values)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count: Int = try! s.front()

			XCTAssertEqual(count, rowCount)
		}
	}

	func testDatabaseInsertPerformance32() {
		self.measure {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);")

			var s = try! db.prepare(sql: "insert into t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

			let values: [Int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]

			let rowCount = 10_000
			for _ in 0..<rowCount {
				try! s.bind(parameterValues: values)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count: Int = try! s.front()

			XCTAssertEqual(count, rowCount)
		}
	}

	func testSQLiteSelectPerformance() {
		self.measure {
			var db: OpaquePointer?
			sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)

			sqlite3_exec(db, "create table t1(a, b);", nil, nil, nil)

			var stmt: OpaquePointer?
			sqlite3_prepare_v2(db, "insert into t1(a, b) values (1, 2);", -1, &stmt, nil)

			let rowCount = 50_000
			for _ in 0..<rowCount {
				sqlite3_step(stmt)
				sqlite3_reset(stmt)
			}

			sqlite3_finalize(stmt)

			sqlite3_prepare_v2(db, "select a, b from t1;", -1, &stmt, nil)

			var result = sqlite3_step(stmt)
			while result == SQLITE_ROW {
				let _ = Int(sqlite3_column_int64(stmt, 0))
				let _ = Int(sqlite3_column_int64(stmt, 1))
				result = sqlite3_step(stmt)
			}

			sqlite3_finalize(stmt)
			sqlite3_close(db)
		}
	}

	func testDatabaseSelectPerformance() {
		self.measure {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			var s = try! db.prepare(sql: "insert into t1(a, b) values (1, 2);")

			let rowCount = 50_000
			for _ in 0..<rowCount {
				try! s.execute()
				try! s.reset()
			}

			s = try! db.prepare(sql: "select a, b from t1;")
			try! s.results { row in
				let _: Int = try row.value(at: 0)
				let _: Int = try row.value(at: 1)
			}
		}
	}

}
