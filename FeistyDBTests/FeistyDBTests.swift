//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
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

	func testBatch() {
		let db = try! Database()

		try! db.batch(sql: "pragma application_id;")
		try! db.batch(sql: "pragma application_id; pragma foreign_keys;")

		XCTAssertThrowsError(try db.batch(sql: "lulu"))

		try! db.batch(sql: "pragma application_id;") { row in
			XCTAssertEqual(row.keys.count, 1)
			XCTAssertEqual(row["application_id"], "0")
		}
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

	func testCustomAggregateFunction() {
		let db = try! Database()

		class IntegerSumAggregateFunction: SQLAggregateFunction {
			func step(_ values: [DatabaseValue]) throws {
			let value = values.first.unsafelyUnwrapped
				switch value {
				case .integer(let i):
					sum += i
				default:
					throw DatabaseError("Only integer values supported")
				}
			}

			func final() throws -> DatabaseValue {
				defer {
					sum = 0
				}
				return DatabaseValue(sum)
			}

			var sum: Int64 = 0
		}

		try! db.addAggregateFunction("integer_sum", arity: 1, IntegerSumAggregateFunction())

		try! db.execute(sql: "create table t1(a);")

		for i in  0..<10 {
			try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [i])
		}

		let s: Int64 = try! db.prepare(sql: "select integer_sum(a) from t1;").front()
		XCTAssertEqual(s, 45)

		let ss: Int64 = try! db.prepare(sql: "select integer_sum(a) from t1;").front()
		XCTAssertEqual(ss, 45)

		try! db.removeFunction("integer_sum", arity: 1)
		XCTAssertThrowsError(try db.prepare(sql: "select integer_sum(a) from t1;"))
	}

	func testCustomAggregateWindowFunction() {
		let db = try! Database()

		class IntegerSumAggregateWindowFunction: SQLAggregateWindowFunction {
			func step(_ values: [DatabaseValue]) throws {
				let value = values.first.unsafelyUnwrapped
				switch value {
				case .integer(let i):
					sum += i
				default:
					throw DatabaseError("Only integer values supported")
				}
			}

			func inverse(_ values: [DatabaseValue]) throws {
				let value = values.first.unsafelyUnwrapped
				switch value {
				case .integer(let i):
					sum -= i
				default:
					throw DatabaseError("Only integer values supported")
				}
			}

			func value() throws -> DatabaseValue {
				return DatabaseValue(sum)
			}

			func final() throws -> DatabaseValue {
				defer {
					sum = 0
				}
				return DatabaseValue(sum)
			}

			var sum: Int64 = 0
		}

		try! db.addAggregateWindowFunction("integer_sum", arity: 1, IntegerSumAggregateWindowFunction())

		try! db.execute(sql: "create table t1(a);")

		for i in  0..<10 {
			try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [i])
		}

		let s = try! db.prepare(sql: "select integer_sum(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t1;")
		let results = s.map { try! $0.leftmostValue() as Int64 }

		XCTAssertEqual(results, [1, 3, 6, 9, 12, 15, 18, 21, 24, 17])

		try! db.removeFunction("integer_sum", arity: 1)
		XCTAssertThrowsError(try db.prepare(sql: "select integer_sum(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t1;"))	}

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

	func testStatementColumns() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b, c);")

		for i in 0..<3 {
			try! db.prepare(sql: "insert into t1(a, b, c) values (?,?,?);").bind(parameterValues: [i, i * 3, i * 5]).execute()
		}

		let statement = try! db.prepare(sql: "select * from t1")
		let cols: [[Int]] = try! statement.columns([0,2])
		XCTAssertEqual(cols[0], [0,1,2])
		XCTAssertEqual(cols[1], [0,5,10])
	}

	func testUUIDExtension() {
		let db = try! Database()
		let statement = try! db.prepare(sql: "select uuid();")
		let s: String = try! statement.front()
		let u = UUID(uuidString: s)
		XCTAssertEqual(u?.uuidString.lowercased(), s.lowercased())
	}

	func testCArrayExtension() {
		let db = try! Database()

		try! db.execute(sql: "create table animals(kind);")

		try! db.prepare(sql: "insert into animals(kind) values ('dog');").execute()
		try! db.prepare(sql: "insert into animals(kind) values ('cat');").execute()
		try! db.prepare(sql: "insert into animals(kind) values ('bird');").execute()
		try! db.prepare(sql: "insert into animals(kind) values ('hedgehog');").execute()

		let pets = [ "dog", "dragon", "hedgehog" ]
		let statement = try! db.prepare(sql: "SELECT * FROM animals WHERE kind IN carray(?1,?2,'char*');")
		try! statement.bind(array: pets, toParameter: 1)
		try! statement.bind(value: pets.count, toParameter: 2)

		let results: [String] = statement.map({try! $0.value(at: 0)})

		XCTAssertEqual([ "dog", "hedgehog" ], results)
	}

	func testDatabaseQueue() {
	}

    func testSQLiteInsertPerformance() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			var db: OpaquePointer?
			sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)

			sqlite3_exec(db, "create table t1(a, b);", nil, nil, nil)

			startMeasuring()

			let rowCount = 50_000
			for i in 0..<rowCount {
				var stmt: OpaquePointer?
				sqlite3_prepare_v2(db, "insert into t1(a, b) values (?, ?);", -1, &stmt, nil)

				sqlite3_bind_int64(stmt, 1, sqlite3_int64(i*2))
				sqlite3_bind_int64(stmt, 2, sqlite3_int64(i*2+1))

				sqlite3_step(stmt)
				sqlite3_finalize(stmt)
			}

			stopMeasuring()

			var stmt: OpaquePointer?
			sqlite3_prepare_v2(db, "select count(*) from t1;", -1, &stmt, nil)
			sqlite3_step(stmt)
			let count = Int(sqlite3_column_int64(stmt, 0))

			sqlite3_finalize(stmt)
			sqlite3_close(db)

			XCTAssertEqual(count, rowCount)
        }
    }

	func testFeistyDBInsertPerformance() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			startMeasuring()

			let rowCount = 50_000
			for i in 0..<rowCount {
				let s = try! db.prepare(sql: "insert into t1(a, b) values (?, ?);")

				try! s.bind(value: i*2, toParameter: 1)
				try! s.bind(value: i*2+1, toParameter: 2)

				try! s.execute()
			}

			stopMeasuring()

			let s = try! db.prepare(sql: "select count(*) from t1;")
			var count = 0
			try! s.results { row in
				count = try row.value(at: 0)
			}

			XCTAssertEqual(count, rowCount)
		}
	}

	func testSQLiteInsertPerformance2() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			var db: OpaquePointer?
			sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)

			sqlite3_exec(db, "create table t1(a, b);", nil, nil, nil)

			var stmt: OpaquePointer?
			sqlite3_prepare_v2(db, "insert into t1(a, b) values (?, ?);", -1, &stmt, nil)

			startMeasuring()

			let rowCount = 50_000
			for i in 0..<rowCount {
				sqlite3_bind_int64(stmt, 1, sqlite3_int64(i*2))
				sqlite3_bind_int64(stmt, 2, sqlite3_int64(i*2+1))

				sqlite3_step(stmt)

				sqlite3_clear_bindings(stmt)
				sqlite3_reset(stmt)
			}

			stopMeasuring()

			sqlite3_finalize(stmt)

			sqlite3_prepare_v2(db, "select count(*) from t1;", -1, &stmt, nil)
			sqlite3_step(stmt)
			let count = Int(sqlite3_column_int64(stmt, 0))

			sqlite3_finalize(stmt)
			sqlite3_close(db)

			XCTAssertEqual(count, rowCount)
		}
	}

	func testFeistyDBInsertPerformance2() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			var s = try! db.prepare(sql: "insert into t1(a, b) values (?, ?);")

			startMeasuring()

			let rowCount = 50_000
			for i in 0..<rowCount {
				try! s.bind(value: i*2, toParameter: 1)
				try! s.bind(value: i*2+1, toParameter: 2)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			stopMeasuring()

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count: Int = try! s.front()

			XCTAssertEqual(count, rowCount)
		}
	}

	func testFeistyDBInsertPerformance31() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);")

			var s = try! db.prepare(sql: "insert into t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

			let values: [Int?] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]

			startMeasuring()

			let rowCount = 10_000
			for _ in 0..<rowCount {
				try! s.bind(parameterValues: values)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			stopMeasuring()

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count: Int = try! s.front()

			XCTAssertEqual(count, rowCount)
		}
	}

	func testFeistyDBInsertPerformance32() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);")

			var s = try! db.prepare(sql: "insert into t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

			let values: [Int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]

			startMeasuring()

			let rowCount = 10_000
			for _ in 0..<rowCount {
				try! s.bind(parameterValues: values)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			stopMeasuring()

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count: Int = try! s.front()

			XCTAssertEqual(count, rowCount)
		}
	}

	func testSQLiteSelectPerformance() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
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

			startMeasuring()

			var result = sqlite3_step(stmt)
			while result == SQLITE_ROW {
				let _ = Int(sqlite3_column_int64(stmt, 0))
				let _ = Int(sqlite3_column_int64(stmt, 1))
				result = sqlite3_step(stmt)
			}

			stopMeasuring()

			sqlite3_finalize(stmt)
			sqlite3_close(db)
		}
	}

	func testFeistyDBSelectPerformance() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			var s = try! db.prepare(sql: "insert into t1(a, b) values (1, 2);")

			let rowCount = 50_000
			for _ in 0..<rowCount {
				try! s.execute()
				try! s.reset()
			}

			s = try! db.prepare(sql: "select a, b from t1;")

			startMeasuring()

			try! s.results { row in
				let _: Int = try row.value(at: 0)
				let _: Int = try row.value(at: 1)
			}

			stopMeasuring()
		}
	}

}
