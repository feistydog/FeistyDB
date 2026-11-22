//
// Copyright (c) 2026 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import XCTest
@testable import FeistyDB

/// Compile-time check that `T` conforms to `Sendable`
private func requireSendable<T: Sendable>(_: T.Type) {}

@available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *)
final class DatabaseQueueTests: XCTestCase {
	/// Returns a URL for a unique database file that is removed (with its WAL and SHM files) after the test
	func temporaryDatabaseURL() -> URL {
		let url = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
		addTeardownBlock {
			for suffix in ["", "-wal", "-shm"] {
				try? FileManager.default.removeItem(atPath: url.path + suffix)
			}
		}
		return url
	}

	override class func setUp() {
		super.setUp()
		// Initialize FeistyDB
		try! FeistyDB.initialize()
	}

	func testQueuesAreSendable() {
		requireSendable(DatabaseQueue.self)
		requireSendable(DatabaseReadQueue.self)
		requireSendable(DatabaseValue.self)
		requireSendable(Database.TransactionCompletion.self)
		requireSendable(Database.SavepointCompletion.self)
	}

	final class Item {   // deliberately not Sendable
		var value: Int
		init(value: Int) { self.value = value }
	}

	func testSyncReturnsNonSendable() throws {
		let dbQueue = try DatabaseQueue(label: "test")
		let item = try dbQueue.sync { db in
			let value: Int = try db.prepare(sql: "select 42;").front()
			return Item(value: value)
		}
		XCTAssertEqual(item.value, 42)
	}

	/// Many concurrent `perform` calls must be serialized: each block's insert and
	/// `lastInsertRowid` read happen without another block running in between.
	func testConcurrentPerform() async throws {
		let dbQueue = try DatabaseQueue(label: "test")
		try await dbQueue.perform { db in
			try db.execute(sql: "create table t1(a);")
		}

		let taskCount = 200
		let rowids = try await withThrowingTaskGroup(of: Int64.self) { group in
			for i in 0 ..< taskCount {
				group.addTask {
					try await dbQueue.perform { db in
						try db.execute(sql: "insert into t1(a) values (?);", parameterValues: [i])
						return db.lastInsertRowid ?? -1
					}
				}
			}
			var rowids = Set<Int64>()
			for try await rowid in group {
				rowids.insert(rowid)
			}
			return rowids
		}

		XCTAssertEqual(rowids, Set(1 ... Int64(taskCount)))

		let count: Int = try await dbQueue.perform { db in
			try db.prepare(sql: "select count(*) from t1;").front()
		}
		XCTAssertEqual(count, taskCount)
	}

	/// The synchronous API must be usable from arbitrary threads.
	func testSyncFromDetachedTasks() async throws {
		let dbQueue = try DatabaseQueue(label: "test")
		try dbQueue.sync { db in
			try db.execute(sql: "create table t1(a);")
		}

		var tasks: [Task<Void, Swift.Error>] = []
		for i in 0 ..< 50 {
			tasks.append(Task.detached { [i] in
				try dbQueue.sync { db in
					try db.execute(sql: "insert into t1(a) values (?);", parameterValues: [i])
				}
			})
		}
		for task in tasks {
			try await task.value
		}

		let count: Int = try dbQueue.sync { db in
			try db.prepare(sql: "select count(*) from t1;").front()
		}
		XCTAssertEqual(count, 50)
	}

	func testAsyncCompletionHandler() async throws {
		let dbQueue = try DatabaseQueue(label: "test")
		let expectation = expectation(description: "completion called")

		dbQueue.async(block: { db in
			try db.execute(sql: "create table t1(a);")
			try db.execute(sql: "insert into t1(a) values (1);")
			let count: Int = try db.prepare(sql: "select count(*) from t1;").front()
			return count
		}, completion: { result in
			XCTAssertEqual(try? result.get(), 1)
			expectation.fulfill()
		})

		await fulfillment(of: [expectation], timeout: 5)
	}

	func testPerformTransaction() async throws {
		let dbQueue = try DatabaseQueue(label: "test")
		try await dbQueue.perform { db in
			try db.execute(sql: "create table t1(a);")
		}

		// Commit
		var completion = try await dbQueue.performTransaction { db in
			try db.execute(sql: "insert into t1(a) values (1);")
			return .commit
		}
		XCTAssertEqual(completion, .commit)

		// Explicit rollback
		completion = try await dbQueue.performTransaction { db in
			try db.execute(sql: "insert into t1(a) values (2);")
			return .rollback
		}
		XCTAssertEqual(completion, .rollback)

		// Thrown error rolls back and is rethrown
		struct TestError: Swift.Error {}
		do {
			try await dbQueue.performTransaction { db in
				try db.execute(sql: "insert into t1(a) values (3);")
				throw TestError()
			}
			XCTFail("Expected error to be rethrown")
		} catch is TestError {
		}

		let values: [Int] = try await dbQueue.perform { db in
			try db.prepare(sql: "select a from t1 order by a;").column(0)
		}
		XCTAssertEqual(values, [1])
	}

	func testPerformSavepoint() async throws {
		let dbQueue = try DatabaseQueue(label: "test")
		try await dbQueue.perform { db in
			try db.execute(sql: "create table t1(a);")
		}

		try await dbQueue.performSavepoint { db in
			try db.execute(sql: "insert into t1(a) values (1);")
			return .release
		}
		try await dbQueue.performSavepoint { db in
			try db.execute(sql: "insert into t1(a) values (2);")
			return .rollback
		}

		let values: [Int] = try await dbQueue.perform { db in
			try db.prepare(sql: "select a from t1 order by a;").column(0)
		}
		XCTAssertEqual(values, [1])
	}

	/// Read queues running concurrently with a write queue on a WAL database
	/// must only ever see committed transactions.
	func testReadQueuesWithConcurrentWrites() async throws {
		let dbQueue = try DatabaseQueue(url: temporaryDatabaseURL(), label: "write")
		try await dbQueue.perform { db in
			let mode: String = try db.prepare(sql: "pragma journal_mode=wal;").front()
			XCTAssertEqual(mode, "wal")
			try db.execute(sql: "create table t1(a);")
		}

		let readQueues = try (0 ..< 4).map { i in
			try DatabaseReadQueue(writeQueue: dbQueue, label: "read \(i)")
		}

		let batchCount = 20
		let batchSize = 10

		try await withThrowingTaskGroup(of: Void.self) { group in
			// Writer: inserts rows in batches, one transaction per batch
			group.addTask {
				for batch in 0 ..< batchCount {
					try await dbQueue.performTransaction { db in
						for i in 0 ..< batchSize {
							try db.execute(sql: "insert into t1(a) values (?);", parameterValues: [batch * batchSize + i])
						}
						return .commit
					}
				}
			}

			// Readers: a partially written batch must never be visible
			for readQueue in readQueues {
				group.addTask {
					for _ in 0 ..< 50 {
						let count: Int = try await readQueue.perform { db in
							try db.prepare(sql: "select count(*) from t1;").front()
						}
						XCTAssertEqual(count % batchSize, 0, "Read queue saw an uncommitted transaction")
					}
				}
			}

			try await group.waitForAll()
		}

		// After all writes, every read queue sees all rows
		for readQueue in readQueues {
			try await readQueue.performReadTransaction { db in
				let count: Int = try db.prepare(sql: "select count(*) from t1;").front()
				XCTAssertEqual(count, batchCount * batchSize)
			}
		}
	}
}
