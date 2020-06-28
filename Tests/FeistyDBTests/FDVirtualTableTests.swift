//
//  FDVirtualTableTests.swift
//  FeistyDBTests
//
//  Created by Jason Jobe on 6/19/20.
//  Copyright © 2020 Feisty Dog, LLC. All rights reserved.
//

import XCTest
import FeistyDB
import CSQLite

class FDVirtualTableTests: XCTestCase {

    let db = try! Database()

    override func setUpWithError() throws {
        try! db.addModule("generate_series", type: SeriesModule.self)
    }

    override func tearDownWithError() throws {
    }

//    func testUserDefaults() {
//        let dict = UserDefaults.standard.dictionaryRepresentation()
//        Swift.print (dict)
//    }
    
    func testUserTable() {
        let db = try! Database()

        try! db.addModule("user", type: UserTable.self)
        let statement = try! db.prepare(sql: "SELECT name, login, home FROM user")

        guard let row = try! statement.firstRow() else { XCTFail(); return }
        Swift.print (try! row.anyValues())
//        XCTAssertEqual((NSFullUserName(), NSUserName(), NSHomeDirectory()),
//                       (row[0].stringValue, row[1].stringValue, row[2].stringValue)
    }

    func testDictionaryTable() {
        let db = try! Database()

        try! db.addModule("env", type: DictionaryTable.self)
        let statement = try! db.prepare(sql: "SELECT key, value FROM env LIMIT 5")
//        let rows = try! statement.columns([0, 1]).map { $0.map { $0.stringValue ?? "" } }
        let rows = try! statement.columns([0, 1])
        Swift.print(#function, rows)
    //        XCTAssertEqual((NSFullUserName(), NSUserName(), NSHomeDirectory()),
    //                       (row[0].stringValue, row[1].stringValue, row[2].stringValue)
        }
    
    func testSeriesVirtualTable_Limit() {

        let statement = try! db.prepare(sql: "SELECT value FROM generate_series LIMIT 5;")
        let results: [Int] = statement.map({try! $0.value(at: 0)})
        XCTAssertEqual(results, [0,1,2,3,4])
    }
    
    func testSeriesVirtualTable_Start() {
        
        let statement = try! db.prepare(sql: "SELECT value FROM generate_series(10) LIMIT 5;")
        let results = statement.map({try! $0.value(at: 0)})
        XCTAssertEqual(results, [10,11,12,13,14])
    }
    
    func testSeriesVirtualTable_AllOptions() {

        let statement = try! db.prepare(sql: "SELECT value FROM generate_series(10, 20, 1) LIMIT 5;")
        let results = statement.map({try! $0.value(at: 0)})
        XCTAssertEqual(results, [10,11,12,13,14])
    }
    
    func testSeriesVirtualTable_Step2() {

        let statement = try! db.prepare(sql: "SELECT value FROM generate_series(10, 20, 2) LIMIT 5;")
        let results = statement.map({try! $0.value(at: 0)})
        XCTAssertEqual(results, [10,12,14,16,18])
    }
}

struct QueryPlanOption: OptionSet {
    struct Info {
        var name: String
        var index: Int
    }
    static var info:[Int:Info] = [:]
    static subscript(_ ndx: Int) -> Info {
        return info[ndx] ?? Info(name: "<INFO>", index: ndx)
    }
    static subscript(_ ndx: Int32) -> Info {
        return info[Int(ndx)] ?? Info(name: "<INFO>", index: Int(ndx))
    }

    let rawValue: Int32

    init(rawValue: Int32) {
        self.rawValue = rawValue
    }

    init(index: Int32) {
        self.rawValue = (1 << index)
    }

    init(_ name: String? = nil, _ ndx: Int) {
        self.rawValue = Int32(1 << ndx)
        if let name = name {
            QueryPlanOption.info[self.index] = Info(name: name, index: self.index)
        }
    }

    var index: Int { rawValue.trailingZeroBitCount }
    var count: Int { rawValue.nonzeroBitCount }

    var info: Info { return Self[index] }
    
    static let value    = QueryPlanOption("value", 0)
    static let start    = QueryPlanOption("start", 1)
    static let stop     = QueryPlanOption("stop", 2)
    static let step     = QueryPlanOption("step", 3)
    static let descending = QueryPlanOption("descending", 4)
    
    static var all: QueryPlanOption = [.value, .start, .stop, .step, .descending]
}

///////////////////////////////////////////////////////
/// A port of the `generate_series` sqlite3 module
/// - seealso: https://www.sqlite.org/src/file/ext/misc/series.c
final class SeriesModule: EponymousVirtualTableModule {    

    final class Cursor: VirtualTableCursor {
        let module: SeriesModule
        var _isDescending = false
        var _rowid: Int64 = 0
        var _value: Int64 = 0
        var _min: Int64 = 0
        var _max: Int64 = 0
        var _step: Int64 = 0

        init(_ module: SeriesModule) {
            self.module = module
            _ = QueryPlanOption.all // Make sure all static options are in info
        }

        func column(_ index: Int32) -> DatabaseValue {
            switch index {
            case 1: return .integer(_min)
            case 2: return .integer(_max)
            case 3: return .integer(_step)
            default:
                return .integer(_value)
            }
        }

        func next() {
            _value += (_isDescending ? -_step : _step)
            _rowid += 1
        }

        func rowid() -> Int64 {
            return _rowid
        }

        func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
            let opt = QueryPlanOption(rawValue: indexNumber)
            func get_opt(p: QueryPlanOption, argc: Int, else v: Int64) -> (Int64, Int) {
                if opt.contains(p), case let .integer(i) = arguments[argc] {
                    return (i, argc + 1)
                }
                return (v, argc)
            }
            
            var argc = 0
            (_min, argc) = get_opt(p: .start, argc: argc, else: 0)
            (_max, argc) = get_opt(p: .stop, argc: argc, else: 100) // 0xffffffff
            (_step, argc) = get_opt(p: .step, argc: argc, else: 1)

            _isDescending = opt.contains(.descending)
            _value = _isDescending ? _max : _min

            if _isDescending && _step > 0 {
                _value -= (_max - _min) % _step
            }
            _rowid = 1
        }

        var eof: Bool {
            _isDescending ? (_value < _min) : (_value > _max)
        }
    }

//    required init(arguments: [String]) {
//        Swift.print("init", arguments)
//    }
    init(database: Database, arguments: [String]) throws {
    }
    
    init(database: Database, arguments: [String], create: Bool) throws {
    }
    
    func destroy() throws {
    }

    var declaration: String {
        "CREATE TABLE x(value,start hidden,stop hidden,step hidden)"
    }

    var options: Database.VirtualTableModuleOptions {
        return [.innocuous]
    }

    func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
        // Inputs
        let constraintCount = Int(indexInfo.nConstraint)
        let constraints = UnsafeBufferPointer<sqlite3_index_constraint>(start: indexInfo.aConstraint, count: constraintCount)

        // Outputs
        let constraintUsage = UnsafeMutableBufferPointer<sqlite3_index_constraint_usage>(start: indexInfo.aConstraintUsage, count: constraintCount)

        var queryPlan = QueryPlanOption()
        _ = QueryPlanOption.all // force Info[] population
        
        for constraint in constraints {
            guard constraint.op == SQLITE_INDEX_CONSTRAINT_EQ else { return .constraint }
            let ndx = constraint.iColumn
            guard constraint.usable != 0 else { continue }
            let opt = QueryPlanOption(index: ndx)
//            Swift.print("plan union", opt.info)
            queryPlan = queryPlan.union(opt)
        }
        Swift.print(queryPlan.info)
        for (ndx, col) in queryPlan.elements().enumerated() {
//            Swift.print("usage", ndx, col.info)
            constraintUsage[col.index - 1].argvIndex = Int32(ndx + 1)
        }
        Swift.print (#line, queryPlan)
        
        if queryPlan.contains(.start) && queryPlan.contains(.stop) {
            // Lower the cost if we also have step
            indexInfo.estimatedCost = 2  - (queryPlan.contains(.step) ? 1 : 0)
            indexInfo.estimatedRows = 1000
            
            // ORDER BY
            if indexInfo.nOrderBy == 1 {
                if indexInfo.aOrderBy[0].desc == 1 {
                    queryPlan = queryPlan.union(.descending)
                }
                indexInfo.orderByConsumed = 1
            }
        }
        else {
            indexInfo.estimatedRows = 2147483647
        }
        // Passed to filter()
//        indexInfo.idxStr =
        indexInfo.idxNum = queryPlan.rawValue

        return .ok
    }

    func openCursor() -> VirtualTableCursor {
        return Cursor(self)
    }
}

public extension OptionSet where RawValue: FixedWidthInteger {

    func elements() -> AnySequence<Self> {
        var remainingBits = rawValue
        var bitMask: RawValue = 1
        return AnySequence {
            return AnyIterator {
                while remainingBits != 0 {
                    defer { bitMask = bitMask &* 2 }
                    if remainingBits & bitMask != 0 {
                        remainingBits = remainingBits & ~bitMask
                        return Self(rawValue: bitMask)
                    }
                }
                return nil
            }
        }
    }
}


/////////////////////////////////////
/*
 #define SQLITE_INTEGER  1
 #define SQLITE_FLOAT    2
 #define SQLITE_BLOB     4
 #define SQLITE_NULL     5
 #ifdef SQLITE_TEXT
 # undef SQLITE_TEXT
 #else
 # define SQLITE_TEXT     3
 #endif
 #define SQLITE3_TEXT     3
 */
public enum SqliteDataType: Int {
    case int = 1
    case float = 2
    case text = 3
    case blob = 4
    case null = 5
}

public struct Column: Equatable {
    let name: String
    var ndx: Int = 0
    let sqlType: SqliteDataType
//    var defaultValue: DatabaseValue
//    var swiftType: DatabaseSerializable.Type
    let pkey: Bool
    let hidden: Bool
    
    var declaration: String {
        "\(name) \(sqlType)\(pkey ? "PRIMARY KEY" : "")\(hidden ? " HIDDEN" : "")"
    }
}

public extension Column {
    static func pkey(_ name: String, _ sqlType: SqliteDataType = .int) -> Column {
        Column(name: name, sqlType: .int, pkey: true, hidden: false)
    }
    static func column(_ name: String, _ stype: SqliteDataType) -> Column {
        Column(name: name, sqlType: stype, pkey: false, hidden: false)
    }
    static func hidden(_ name: String, _ stype: SqliteDataType) -> Column {
        Column(name: name, sqlType: stype, pkey: false, hidden: true)
    }
}

open class BaseEponymousTable: EponymousVirtualTableModule {
    
    required public init(database: Database, arguments: [String]) {
    }

    public required init(database: Database, arguments: [String], create: Bool) throws {
    }
    
    open func destroy() throws {
    }

    open var columns: [Column] = []
       
    lazy public var declaration: String = {
           var str: String = "CREATE TABLE x("
           for col in columns {
               Swift.print("\t\(col.declaration)", separator: "", terminator: "", to: &str)
               if col != columns.last {
                   Swift.print(",\n", separator: "", terminator: "", to: &str)
               }
           }
           Swift.print("\n)", separator: "", terminator: "", to: &str)
           return str
       }()


    public var options: Database.VirtualTableModuleOptions = [.innocuous]

    open func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
        .ok
    }

    open func openCursor() -> VirtualTableCursor {
        BaseCursor(self)
    }
}

open class BaseCursor<B: BaseEponymousTable>: VirtualTableCursor {
     var table: B
     var _rowid: Int64 = 0
     var _count = 1 // Row count

     init (_ table: B) {
         self.table = table
     }
     
    public func column(_ index: Int32) -> DatabaseValue {
         let col = table.columns[Int(index)]
         switch col.name {
         default:
             return .null
         }
     }

    public func next() {
         _rowid += 1
     }

    public func rowid() -> Int64 {
         _rowid
     }

    public func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
         _rowid = 1
     }

    public var eof: Bool {
         _rowid > _count
     }
 }

final class UserTable: EponymousVirtualTableModule {
    
    final class Cursor: VirtualTableCursor {
        var table: UserTable
        var _rowid: Int64 = 0
        var _count = 1 // Row count

        init (_ table: UserTable) {
            self.table = table
        }
        
        func column(_ index: Int32) -> DatabaseValue {
            let col = table.columns[Int(index)]
            switch col.name {
            case "name": return .text(NSFullUserName())
            case "login": return .text((NSUserName()))
            case "home": return .text(NSHomeDirectory())
            default:
                return .null
            }
        }

        func next() {
            _rowid += 1
        }

        func rowid() -> Int64 {
            _rowid
        }

        func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
            _rowid = 1
        }

        var eof: Bool {
            _rowid > _count
        }
    }

    required init(database: Database, arguments: [String]) {
    }

    init(database: Database, arguments: [String], create: Bool) throws {
    }
    
    func destroy() throws {
    }

    lazy var columns: [Column] = {
           var cols:[Column] = [
            .column("login", .text),
            .column("name", .text),
            .column("home", .text)
        ]
        for ndx in 0..<cols.count {
               cols[ndx].ndx = ndx
           }
           return cols
       }()
       
       lazy var declaration: String = {
           var str: String = "CREATE TABLE x("
           for col in columns {
               Swift.print("\t\(col.declaration)", separator: "", terminator: "", to: &str)
               if col != columns.last {
                   Swift.print(",\n", separator: "", terminator: "", to: &str)
               }
           }
           Swift.print("\n)", separator: "", terminator: "", to: &str)
           return str
       }()


    var options: Database.VirtualTableModuleOptions {
        [.innocuous]
    }

    func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
        indexInfo.estimatedRows = 1
        indexInfo.idxFlags |= SQLITE_INDEX_SCAN_UNIQUE
        return .ok
    }

    func openCursor() -> VirtualTableCursor {
        Cursor(self)
    }
}

//////////////////////////////////

final class DictionaryTable: EponymousVirtualTableModule {
    
    var dict: [String: String] = ProcessInfo.processInfo.environment
    
    required init(database: Database, arguments: [String]) {
    }

    init(database: Database, arguments: [String], create: Bool) throws {
    }
    
    func destroy() throws {
    }

    lazy var columns: [Column] = {
           var cols:[Column] = [
            .column("key", .text),
            .column("value", .text),
        ]
        for ndx in 0..<cols.count {
               cols[ndx].ndx = ndx
           }
           return cols
       }()
       
       lazy var declaration: String = {
           var str: String = "CREATE TABLE x("
           for col in columns {
               Swift.print("\t\(col.declaration)", separator: "", terminator: "", to: &str)
               if col != columns.last {
                   Swift.print(",\n", separator: "", terminator: "", to: &str)
               }
           }
           Swift.print("\n)", separator: "", terminator: "", to: &str)
           return str
       }()


    var options: Database.VirtualTableModuleOptions {
        [.innocuous]
    }

    func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
        .ok
    }

    func openCursor() -> VirtualTableCursor {
        Cursor(self)
    }
}

extension DictionaryTable {

    final class Cursor: VirtualTableCursor {
        
        var table: DictionaryTable
        var _rowid: Int64 = 0
        lazy var _count: Int = table.dict.count
        
        var row: (key: String, value: String) = ("", "")
        var index: Dictionary<String, String>.Index
        
        init (_ table: DictionaryTable) {
            self.table = table
            self.index = table.dict.startIndex
        }
        
        func column(_ index: Int32) -> DatabaseValue {
            switch index {
            case 1: return .text(row.key)
            case 2: return .text(row.value)
            default:
                return .text(row.key)
            }

//            let col = table.columns[Int(index)]
//            switch col.name {
//            case "key": return .text(row.key)
//            case "value": return .text(row.value)
//            default:
//                return .integer(_rowid)
//            }
        }

        func next() {
            row = table.dict[index]
            index = table.dict.index(after: index)
            _rowid += 1
        }

        func rowid() -> Int64 {
            _rowid
        }

        func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
            self.index = table.dict.startIndex
            _rowid = 0
        }

        var eof: Bool {
            _rowid > _count
        }
    }
}

