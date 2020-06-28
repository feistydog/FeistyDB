//
//  SampleVTables.swift
//  CSQLite
//
//  Created by Jason Jobe on 6/26/20.
//

import Foundation
import FeistyDB
import CSQLite

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
