//
//  CalendarTable.swift
//  fdb
//
//  Created by Jason Jobe on 7/23/20.
//  Copyright © 2020 Jason Jobe. All rights reserved.
//
import Foundation
import CSQLite
import FeistyDB

final public class CalendarModule: BaseTableModule {
    
    public enum Column: Int32, ColumnIndex {
        case date, weekday, day, week, month, year, start, stop, step
    }
    public override var declaration: String {
        "CREATE TABLE x(date, weekday, day, week, month, year, start HIDDEN, stop HIDDEN, step HIDDEN)"
    }

    var date_fmt: DateFormatter
    
    required init(database: Database, arguments: [String], create: Bool) throws {
        Swift.print (#function, arguments)
        date_fmt = DateFormatter()
        date_fmt.dateFormat = "yyyy-MM-dd"
        try super.init(database: database, arguments: arguments, create: create)
    }
    
    required public init(database: Database, arguments: [String]) throws {
        Swift.print (#function, arguments)
        date_fmt = DateFormatter()
        date_fmt.dateFormat = "yyyy-MM-dd"
        try super.init(database: database, arguments: arguments, create: false)
    }
    
    public override func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
        
        guard var info = FilterInfo(&indexInfo) else { return .constraint }
        if info.contains(Column.start) && info.contains(Column.stop) {
            indexInfo.estimatedCost = 2  - (info.contains(Column.step) ? 1 : 0)
            indexInfo.estimatedRows = 1000
        }
        else {
            indexInfo.estimatedRows = 2147483647
        }
        
        if let arg = info.argv.first(where: { $0.col_ndx == Column.date.rawValue } ),
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
    func contains(_ col: CalendarModule.Column) -> Bool {
        argv.contains(where: { $0.col_ndx == col.rawValue} )
    }
}

//extension DatabaseValue {
//    static func integer(for i: Int) -> DatabaseValue {
//        DatabaseValue.integer(Int64(i))
//    }
//}

extension CalendarModule {
    final class Cursor: BaseTableModule.Cursor<CalendarModule> {
        
        var calendar: Calendar
        var start: Date
        var end: Date
        var current: Date?
        var step: Calendar.Frequency = .daily
        var date_fmt: DateFormatter { module.date_fmt }
        
        public override init(_ table: CalendarModule, filter: FilterInfo?)
        {
            self.calendar = Calendar.current
            self.start = Date()
            self.end = .distantFuture
            self.current = start
            super.init(table, filter: filter)
        }

        override func column(_ index: Int32) -> DatabaseValue {
            // "CREATE TABLE x(date, weekday, day, week, month, year, start, stop, step)"
            func dbvalue(_ date: Date, _ comp: Calendar.Component) -> DatabaseValue {
                .integer(Int64(calendar.component(comp, from: date)))
            }
            guard let date = current, let col = Column(rawValue: index) else { return .null }
            switch col {
                case .date:     return .text(date_fmt.string(from: date))
                case .weekday:  return dbvalue(date, .weekday)
                case .day:      return dbvalue(date, .day)
                case .week:     return dbvalue(date, .weekOfYear)
                case .month:    return dbvalue(date, .month)
                case .year:     return dbvalue(date, .year)
                    
                //  HIDDEN
                case .start:    return .text(date_fmt.string(from: start))
                case .stop:     return .text(date_fmt.string(from: end))
                case .step:     return .text(step.name)
             }
        }
        
        override func next() {
            _rowid += 1
            current = step.nextDate(from: current, in: calendar) ?? current
        }
                
        override func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
            defer { module.clearFilters() }
            guard let filterInfo = filterInfo ?? module.filters[Int(indexNumber)]
            else { return }
            _rowid = 1
            start = Date()
            end = .distantFuture
            step = .daily
            
            // DEBUG
            Swift.print(
                filterInfo.describe(with: Column.allCases.map {String(describing:$0)},
                                    values: arguments))

            for farg in filterInfo.argv {
                switch (Column(rawValue: farg.col_ndx), arguments[Int(farg.arg_ndx)]) {
                    case (.start, let .text(argv)): start  = date_fmt.date(from: argv) ?? start
                    case (.stop,  let .text(argv)): end  = date_fmt.date(from: argv) ?? end
                    case (.step,  let .text(argv)): step = Calendar.Frequency.named(argv) ?? step
                        
                    case (.date,  let .text(argv)):
                        guard let date = date_fmt.date(from: argv)  else { continue }
                        switch farg.op_str {
                            case "=":
                                start = date
                                end = date
                                current = date
                            case "<", "<=":
                                end = date
                            case ">", ">=":
                                start = date
                            default:
                                break
                        }

                    default:
                        break
                }
            }
            current = filterInfo.isDescending ? end : start
        }
        
        override var eof: Bool {
            // HARD LIMIT of total days for 100 year span is 36_500
            if let date = current, date > end || _rowid > 36_500 {
                return true
            }
            return false
        }
    }
}
