//
//  File.swift
//  
//
//  Created by Jason Jobe on 7/9/20.
//

import Foundation

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
