//
//  FDBExtensionsTest.swift
//  fdb
//
//  Created by Jason Jobe on 4/15/20.
//  Copyright © 2020 Jason Jobe. All rights reserved.
//

import XCTest
@testable import CSQLite
@testable import FeistyDB
@testable import FeistyExtensions

class FDBExtensionsTest: XCTestCase {

    var db: Database!
    
    override func setUpWithError() throws {
        db = try Database()
        let sql = """
            create table stuff (id, key, tags);
        """
        try db.execute(sql: sql)
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testJSONWithPlist() throws {
        
        let plist: [String:Any] = [
            "a": 1,
            "b": [2, 3, 4]
        ]
        try db.insert(into: "stuff", from: ["id": 1, "key": "lock", "tags": plist])
        
        try db.results(sql: "select tags from stuff where id = 1") { row in
             let tags: [String:Any] = try row.value(at: 0)
             Swift.print(tags)
             XCTAssert(tags.count == 2)
             XCTAssert(tags["b"] is [Int])
         }
    }
    
     func testJSONAnyArrays() throws {
         
         try db.insert(into: "stuff", from: ["id": 1, "key": "lock", "tags": [1, "two", 3]])

         try db.results(sql: "select tags from stuff where id = 1") { row in
             let tags: [Any] = try row.value(at: 0)
             XCTAssert(tags.count == 3)
             XCTAssert(tags[0] is Int)
             XCTAssert(tags[1] is String)
         }
      }

    func testJSONArrays() throws {
        
        try db.insert(into: "stuff", from: ["id": 1, "key": "lock", "tags": ["t1", "t2"]])
        try db.insert(into: "stuff", from: ["id": 2, "key": "lock", "tags": [1, 2, 3]])

        try db.results(sql: "select tags from stuff where id = 1") { row in
            let tags: [String] = try row.value(at: 0)
            XCTAssert(tags.count == 2)
            XCTAssert(tags[0] == "t1")
        }
   
        try db.results(sql: "select tags from stuff where id = 2") { row in
            let tags: [Int] = try row.value(at: 0)
            XCTAssert(tags.count == 3)
            XCTAssert(tags[0] == 1)
        }
    }

    func testPerformanceExample() throws {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
        }
    }

}
