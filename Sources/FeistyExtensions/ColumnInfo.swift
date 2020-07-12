//
//  File.swift
//  
//
//  Created by Jason Jobe on 6/28/20.
//

import Foundation
import FeistyDB

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
    
    public static func == (lhs: Column, rhs: Column) -> Bool {
        lhs.name == rhs.name
    }
    
    public let name: String
    public var ndx: Int = 0
    public let sqlType: SqliteDataType
    public let sqlDeclaraton: String = ""
    
    //    var defaultValue: DatabaseValue
    //    var swiftType: DatabaseSerializable.Type
    
    public var _read: ((DatabaseValue) -> Any?)?
    public var _write: ((Any?) -> DatabaseValue)?

    // Bool Properties - Align at end of struct
    public let isPrimarykey: Bool
    public let hidden: Bool

    public func write(_ value: DatabaseSerializable?) -> DatabaseValue {
        value?.serialized() ?? .null
    }
    public func read<A>(_ dbv: DatabaseValue) -> A? {
        (_read?(dbv) ?? dbv.anyValue) as? A
    }
    public func read(_ dbv: DatabaseValue) -> Any? {
        return (_read?(dbv) ?? dbv.anyValue)
    }

    public func format(_ value: DatabaseSerializable?) -> String {
        guard let value = value else { return "" }
        return "\(value)"
    }
    
    public var declaration: String {
        "\(name) \(sqlType)\(isPrimarykey ? "PRIMARY KEY" : "")\(hidden ? " HIDDEN" : "")"
        + " \(sqlDeclaraton)"
    }
}

public extension Column {
    static func pkey(_ name: String, _ sqlType: SqliteDataType = .int) -> Column {
        Column(name: name, sqlType: .int, isPrimarykey: true, hidden: false)
    }
    static func column(_ name: String, _ stype: SqliteDataType) -> Column {
        Column(name: name, sqlType: stype, isPrimarykey: false, hidden: false)
    }
    static func hidden(_ name: String, _ stype: SqliteDataType) -> Column {
        Column(name: name, sqlType: stype, isPrimarykey: false, hidden: true)
    }
    
    // day, date, datetime, timestamp
    // image, color
    // json, tags
}
