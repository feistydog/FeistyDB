//
//  File.swift
//  
//
//  Created by Jason Jobe on 7/9/20.
//

import Foundation

public extension Calendar {
    enum Frequency: CaseIterable {
        case daily, weekly, biweekly,
             monthly, bimonthly, quarterly, yearly
        
        public var name: String { "\(self)" }
        public var step: Int { (self == .biweekly ? 2 : 1) }
        
        public static func named(_ name: String) -> Frequency? {
            Frequency.allCases.first(where: { $0.name == name })
        }
        
        public func nextDate(from date: Date?, in cal: Calendar = Calendar.current) -> Date? {
            guard let date = date else { return nil }
            var dateComponent = DateComponents()
            switch self {
                //                case .once: return date
                case .daily: dateComponent.day = 1
                case .weekly: dateComponent.weekOfYear = 1
                case .biweekly: dateComponent.weekOfYear = 2
                case .monthly: dateComponent.month = 1
                case .bimonthly: dateComponent.month = 2
                case .quarterly: dateComponent.quarter = 1
                case .yearly: dateComponent.year = 1
            }
            return Calendar.current.date(byAdding: dateComponent, to: date)
        }
    }
}

public extension Calendar {
    
    func dateRange(start: Date,
                   end: Date,
                   stepUnits: Calendar.Component,
                   stepValue: Int) -> DateRange {
        let dateRange = DateRange(calendar: self,
                                  start: start,
                                  end: end,
                                  stepUnits: stepUnits,
                                  stepValue: stepValue)
        return dateRange
    }
    
}


public struct DateRange: Sequence, IteratorProtocol {
    
    var calendar: Calendar
    var start: Date
    var end: Date
    var stepUnits: Calendar.Component
    var stepValue: Int
    
    private var multiplier: Int
    
    
    public init(calendar: Calendar, start: Date, end: Date, stepUnits: Calendar.Component, stepValue: Int) {
        self.calendar = calendar
        self.start = start
        self.end = end
        self.stepUnits = stepUnits
        self.stepValue = stepValue
        self.multiplier = 0
    }
    
    // https://developer.apple.com/documentation/foundation/nscalendar/1416165-nextdate
    
    mutating public func next() -> Date? {
        guard let nextDate = calendar.date(byAdding: stepUnits,
                                           value: stepValue * multiplier,
                                           to: start,
                                           wrappingComponents: false) else {
            return nil
        }
        
        guard nextDate < end else {
            return nil
        }
        
        multiplier += 1
        return nextDate
    }
    
}
