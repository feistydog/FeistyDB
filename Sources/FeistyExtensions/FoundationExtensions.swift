//
//  File.swift
//  
//
//  Created by Jason Jobe on 9/23/20.
//

import Foundation

public func cmpBounds<C: Comparable>(_ op: String, _ val: C,
                           in rng: (min: C, max: C)) -> (min: C, max: C)
{
    switch op {
        case "=": return (val, val)
        case "<", "<=":  return (rng.min, val)
        case ">", ">=":  return (val, rng.max)
        default: return rng
    }
}

public extension Collection {
    /// Returns the element at the specified index if it is within bounds, otherwise nil.
    subscript (safe index: Index) -> Element? {
        return indices.contains(index) ? self[index] : nil
    }
    subscript (safe index: Index, else value: Element) -> Element {
        return indices.contains(index) ? self[index] : value
    }
}
