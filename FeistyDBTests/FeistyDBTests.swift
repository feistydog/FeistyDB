//
// Copyright (c) 2015 - 2017 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import XCTest
@testable import FeistyDB

class FeistyDBTests: XCTestCase {

    override func setUp() {
        super.setUp()
    }
    
    override func tearDown() {
        super.tearDown()
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

	func testUUIDExtension() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(u text default (uuid4()));")
		try! db.execute(sql: "insert into t1 default values;")

		let u: UUID? = try! db.prepare(sql: "select * from t1 limit 1;").front()

		XCTAssertNotNil(u)
	}

	func testCustomCollation() {
		let db = try! Database()

		try! db.add(collation: "reversed", { (a, b) -> ComparisonResult in
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

		try! db.add(function: "rot13", arity: 1) { values in
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

		try! db.remove(function: "rot13", arity: 1)
		XCTAssertThrowsError(try db.prepare(sql: "select rot13(a) from t1;"))
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
