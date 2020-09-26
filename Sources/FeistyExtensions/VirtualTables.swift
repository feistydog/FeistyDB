//
//  File.swift
//  
//
//  Created by Jason Jobe on 7/5/20.
//

import Foundation
import FeistyDB
import CSQLite


public struct QueryPlanOption: OptionSet {
    struct Info {
        var name: String
        var index: Int
//        var option: QueryPlanOption
    }
    static var info:[Int:Info] = [:]
    static subscript(_ ndx: Int) -> Info {
        return info[ndx] ?? Info(name: "<INFO>", index: ndx)
    }
    static subscript(_ ndx: Int32) -> Info {
        return info[Int(ndx)] ?? Info(name: "<INFO>", index: Int(ndx))
    }
    
    public let rawValue: Int32
    
    public init(rawValue: Int32) {
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

///////////////////////////////////////////////////////
/// A port of the `generate_series` sqlite3 module
/// - seealso: https://www.sqlite.org/src/file/ext/misc/series.c
/*
public class DateSeriesModule: EponymousVirtualTableModule {
    
    final class Cursor: VirtualTableCursor {
        let module: DateSeriesModule
        var _isDescending = false
        var _rowid: Int64 = 0
//        var slots: [DatabaseValue] = []
        var _value: Date
        var _start: Date
        var _stop: Date
        var _step: TimeInterval = 1

        init(_ module: DateSeriesModule) {
            self.module = module
            _ = QueryPlanOption.all // Make sure all static options are in info
        }
        
        func column(_ index: Int32) -> DatabaseValue {
//            slots[Int(index)]
            switch index {
                case 1: return _start
                case 2: return _stop
                case 3: return _step
                default:
                    return _value
            }
        }
        
        func next() {
//            _value += (_isDescending ? -_step : _step)
            _rowid += 1
        }
        
        func rowid() -> Int64 {
            return _rowid
        }
        
        func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
            let opt = QueryPlanOption(rawValue: indexNumber)
            func get_opt(p: QueryPlanOption, argc: Int, else v: DatabaseValue) -> (DatabaseValue, Int) {
                if opt.contains(p), case let .integer(i) = arguments[argc] {
                    return (v, argc + 1)
                }
                return (v, argc)
            }
            
            var argc = 0
//            for p in slots where opt.contains(p.index) {
//                (slots[1], argc) = get_opt(p: p, argc: argc, else: .integer(0))
//            }
            
            (slots[1], argc) = get_opt(p: .start, argc: argc, else: .integer(0))
            (slots[2], argc) = get_opt(p: .stop, argc: argc, else: .integer(0))
            (slots[3], argc) = get_opt(p: .step, argc: argc, else: .integer(0))
            
            _isDescending = opt.contains(.descending)
            _value = _isDescending ? _stop : _start
            
            _rowid = 1
        }
        
        var eof: Bool {
            _isDescending ? (_value < _start) : (_value > _stop)
        }
    }
    
    //    required init(arguments: [String]) {
    //        Report.print("init", arguments)
    //    }
    required public init(database: Database, arguments: [String]) throws {
    }
    
    public required init(database: Database, arguments: [String], create: Bool) throws {
    }
    
    public func destroy() throws {
    }
    
    public var declaration: String {
        "CREATE TABLE x(day, month, year, start hidden,stop hidden,step hidden)"
    }
    
    public var options: Database.VirtualTableModuleOptions {
        return [.innocuous]
    }
    
    public func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
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
            //            Report.print("plan union", opt.info)
            queryPlan = queryPlan.union(opt)
        }
        Report.print(queryPlan.info)
        for (ndx, col) in queryPlan.elements().enumerated() {
            //            Report.print("usage", ndx, col.info)
            constraintUsage[col.index - 1].argvIndex = Int32(ndx + 1)
        }
        //        report (#line, queryPlan)
        
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
    
    public func openCursor() -> VirtualTableCursor {
        return Cursor(self)
    }
}
*/
