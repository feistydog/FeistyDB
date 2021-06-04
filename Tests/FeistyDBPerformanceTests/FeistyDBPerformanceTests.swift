//
// Copyright (c) 2015 - 2021 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import XCTest
import FeistyDB

class FeistyDBPerformanceTests: XCTestCase {
	override class func setUp() {
		super.setUp()
		// It's necessary to call sqlite3_initialize() since SQLITE_OMIT_AUTOINIT is defined
		XCTAssertNoThrow(try SQLite.initialize())
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
