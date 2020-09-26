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

public func adjustBounds<C: Comparable>(_ op: String, _ val: C,
                                     in rng: (min: C, max: C)) -> (min: C, max: C)
{
    switch op {
        case "=": return (val, val)
        case "<", "<=":  return (rng.min, (val < rng.max) ? val : rng.max)
        case ">", ">=":  return ((val > rng.min ? val : rng.min), rng.max)
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

// MARK: report logging

public var Report = Reporter()

public struct Reporter {
    public var debug: Bool = false
    
    public func print(_ items: Any?..., separator: String = " ", terminator: String = "\n")
    {
        guard debug else { return }
        var str = ""
        print(argv: items, separator: separator, terminator: terminator, to: &str)
        Swift.print(str)
    }

    public func print<Target>(_ items: Any?..., separator: String = " ", terminator: String = "\n", to output: inout Target) where Target : TextOutputStream
    {
        guard debug else { return }
        var str = ""
        print(argv: items, separator: separator, terminator: terminator, to: &str)
        Swift.print(str)
    }

    public func print<Target>(argv items: [Any?], separator: String = " ", terminator: String = "\n", to output: inout Target) where Target : TextOutputStream
    {
        guard debug else { return }

        func pr1(_ any: Any) { Swift.print(any, terminator: "", to: &output) }
        
        let end = items.count - 1
        for (ndx, item) in items.enumerated() {
            guard let arg = item else {
                pr1("nil")
                continue
            }
            pr1(arg)
            //        let key = String(describing: type(of: arg))
            //        if let fmtr = fmts[key],
            //           let str = fmtr.string(for: arg) {
            //            pr1(str)
            //        } else {
            //            pr1(arg)
            //        }
            if ndx < end { pr1(separator) }
        }
        Swift.print ("", terminator: terminator, to: &output)
    }

}



