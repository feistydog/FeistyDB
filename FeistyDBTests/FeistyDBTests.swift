/*
 *  Copyright (C) 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

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

		let rowCount = 10000

		for i in 0..<rowCount {
			try! db.execute(sql: "insert into t1(a, b) values (?, ?);", [i, i])
		}

		let s = try! db.prepare(sql: "select count(*) from t1;")
		var count = 0
		try! s.results { row in
			count = try! row.column(0)
		}

		XCTAssertEqual(count, rowCount)
    }

	func testDatabaseQueue() {
		let queue = try! DatabaseQueue()

		queue.sync { db in
			try! db.execute(sql: "create table t1(a, b);")

			let rowCount = 10000

			for i in 0..<rowCount {
				try! db.execute(sql: "insert into t1(a, b) values (?, ?);", [i, i])
			}

			let s = try! db.prepare(sql: "select count(*) from t1;")
			var count = 0
			try! s.results { row in
				count = try! row.column(0)
			}

			XCTAssertEqual(count, rowCount)
		}
	}

	func testConcurrentDatabaseQueue() {
//		let queue = try! ConcurrentDatabaseQueue()
//
//		let rowCount = 10000
//
//		queue.write { db in
//			try! db.execute(sql: "create table t1(a, b);")
//
//			for i in 0..<rowCount {
//				try! db.execute(sql: "insert into t1(a, b) values (?, ?);", [i, i])
//			}
//		}
//
//		queue.read { db in
//			let s = try! db.prepare(sql: "select count(*) from t1;")
//			var count = 0
//			try! s.results { row in
//				count = try! row.column(0)
//			}
//
//			XCTAssertEqual(count, rowCount)
//		}
	}

    func testPerformanceExample() {
        self.measure {
        }
    }
    
}
