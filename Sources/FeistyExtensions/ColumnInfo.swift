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
    public let name: String
    public var ndx: Int = 0
    public let sqlType: SqliteDataType
    //    var defaultValue: DatabaseValue
    //    var swiftType: DatabaseSerializable.Type
    public let isPrimarykey: Bool
    public let hidden: Bool
    
    public func read(_ dbv: DatabaseValue) -> Any? {
        return dbv.anyValue
    }
    
    public var declaration: String {
        "\(name) \(sqlType)\(isPrimarykey ? "PRIMARY KEY" : "")\(hidden ? " HIDDEN" : "")"
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
}
