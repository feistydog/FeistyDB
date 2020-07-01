//
//  SampleVTables.swift
//  FeistyDB
//
//  Created by Jason Jobe on 6/26/20.
//

import Foundation
import FeistyDB
import FeistyExtensions
import CSQLite

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
//        Swift.print (#line, queryPlan)
        
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

open class DictionaryTable: EponymousVirtualTableModule {
    
    public var dict: [String: String] = [:]
    
    required public init(database: Database, arguments: [String]) {
    }
    
    required public init(database: Database, arguments: [String], create: Bool) throws {
    }
    
    public func destroy() throws {
    }
    
    lazy public var columns: [Column] = {
        var cols:[Column] = [
            .column("key", .text),
            .column("value", .text),
        ]
        for ndx in 0..<cols.count {
            cols[ndx].ndx = ndx
        }
        return cols
    }()
    
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
    
    
    public var options: Database.VirtualTableModuleOptions {
        [.innocuous]
    }
    
    public func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
        .ok
    }
    
    public func openCursor() -> VirtualTableCursor {
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
                case 0: return .text(row.key)
                case 1: return .text(row.value)
                default:
                    return .null
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
            index = table.dict.index(after: index)
            row = table.dict[index]
            _rowid += 1
        }
        
        func rowid() -> Int64 {
            _rowid
        }
        
        func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
            self.index = table.dict.startIndex
            row = table.dict[index]
            _rowid = 1
        }
        
        var eof: Bool {
            _rowid > _count
        }
    }
}
