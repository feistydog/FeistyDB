//
//  CalendarVTable.swift
//  fdb
//
//  Created by Jason Jobe on 8/17/20.
//
import Foundation
import CSQLite
import FeistyDB

public final class SeriesModule: BaseTableModule {

    public enum Column: Int32, ColumnIndex, CaseIterable {
        case value, start, stop, step
    }

    public override var declaration: String {
        "CREATE TABLE x(value,start hidden,stop hidden,step hidden)"
    }
    
    public override func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
        
        guard var info = FilterInfo(&indexInfo) else { return .constraint }
        if info.contains(Column.start) && info.contains(Column.stop) {
            indexInfo.estimatedCost = 2  - (info.contains(Column.step) ? 1 : 0)
            indexInfo.estimatedRows = 1000
        }
        else {
            indexInfo.estimatedRows = Int64.max // 2147483647
        }
        
        if let arg = info.argv.first(where: { $0.col_ndx == Column.value.rawValue } ),
           arg.op_str == "=" {
            indexInfo.estimatedRows = 1
            indexInfo.idxFlags = SQLITE_INDEX_SCAN_UNIQUE
        }

        indexInfo.idxNum = add(&info)
        return .ok
    }
    
    public override func openCursor() -> VirtualTableCursor {
        return Cursor(self, filter: filters.last)
    }
}

// Extenstion to interface w/ SeriesModule
extension FilterInfo {
    func contains(_ col: SeriesModule.Column) -> Bool {
        argv.contains(where: { $0.col_ndx == col.rawValue} )
    }
}

extension SeriesModule {
    final class Cursor: BaseTableModule.Cursor<SeriesModule> {
        let max_rows = 10_000 // Safety Net b/c default max is TOO BIG
        var _value: Int64 = 0
        var _min: Int64 = 0
        var _max: Int64 = 0
        var _step: Int64 = 0
        
        override func column(_ index: Int32) -> DatabaseValue {
            let col = Column(rawValue: index)
            switch col {
                case .value: return .integer(_value)
                case .start: return .integer(_min)
                case .stop:  return .integer(_max)
                case .step:  return .integer(_step)
                default:     return nil
            }
        }
        
        override func next() {
            _value += (isDescending ? -_step : _step)
            _rowid += 1
        }
        override var eof: Bool {
            guard _rowid <= max_rows else { return true }
            return isDescending ? (_value < _min) : (_value > _max)
        }

        override func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
            defer { module.clearFilters() }
            guard let filterInfo = filterInfo ?? module.filters[Int(indexNumber)]
            else { return }
            _rowid = 1
            _min = 0
            _max = 0xffffffff
            _step = 1
            
            // DEBUG
            Swift.print(
                filterInfo.describe(with: Column.allCases.map {String(describing:$0)},
                                           values: arguments))
            
            for farg in filterInfo.argv.reversed() {
                switch (Column(rawValue: farg.col_ndx), arguments[Int(farg.arg_ndx)]) {
                    case (.start, let DatabaseValue.integer(arg)): _min  = arg
                    case (.stop,  let DatabaseValue.integer(arg)): _max  = arg
                    case (.step,  let DatabaseValue.integer(arg)): _step = arg
                    case (.value, let DatabaseValue.integer(arg)):
                        switch farg.op_str {
                            case "=":
                                _min = arg
                                _max = arg
                                _value = arg
                            case "<", "<=":
                                _max = arg
                            case ">", ">=":
                                _min = arg
                            default:
                                break
                        }
                    default:
                        break
                }
            }
            
            if arguments.contains(where: { return $0 == .null ? true : false }) {
                _min = 1
                _max = 0
            }
            
            _value = isDescending ? _max : _min
            if isDescending && _step > 0 {
                _value -= (_max - _min) % _step
            }
        }
    }
}
