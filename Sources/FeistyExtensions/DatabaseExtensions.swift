//
//  DatabaseExtensions.swift
//  FeistDb - WildThink
//
//  Created by Jason Jobe on 4/12/20.
//  Copyright © 2020 Jason Jobe. All rights reserved.
//
import Foundation
import FeistyDB
import CSQLite

struct SQLFormatError: Error {
    var message: String
    var details: String?
    
    static var tooFewParameters: SQLFormatError {
        SQLFormatError(message: "Too Few Parameters", details: nil)
    }
    static var tooManyParameters: SQLFormatError {
        SQLFormatError(message: "Too Many Parameters", details: nil)
    }
    static var unrecognizedOption: SQLFormatError {
        SQLFormatError(message: "Unrecognized Option", details: nil)
    }
}
    
extension DatabaseValue {
    
    public var anyValue: Any? {
        switch(self) {
        case .integer(let i1): return i1
        case .float(let f1): return f1
        case .text(let t1): return t1
        case .blob(let b1): return b1
        case .null: return nil
        }
    }
    
    public var stringValue: String? {
        guard case let DatabaseValue.text(value) = self else { return nil }
        return value
    }
    public var int64Value: Int64? {
        guard case let DatabaseValue.integer(value) = self else { return nil }
        return value
    }
    public var doubleValue: Double? {
        guard case let DatabaseValue.float(value) = self else { return nil }
        return value
    }
    public var dataValue: Data? {
        guard case let DatabaseValue.blob(value) = self else { return nil }
        return value
    }

}

extension Database {
    
//    public func load(sql: String) throws {
//        var errmsg: UnsafeMutablePointer<Int8>?
//        sqlite3_exec(db, sql, nil, nil, &errmsg)
//        if let errmsg = errmsg {
//            let msg = String(cString: errmsg)
//            throw SQLiteError(msg, code: 0)
//        }
//    }

    public func get<A:ColumnConvertible>(_ col: String, from table: String, id: Int, as type: A.Type = A.self) throws -> A? {
        let sql = "SELECT \(format(column:col)) FROM \(table) WHERE id = \(id) LIMIT 1"
        var value: A?
        try prepare(sql: sql).results {
            value = try $0.value(at: 0)
        }
        return value
    }

    public func select(_ cols: [String], from table: String, where test: String? = nil, _ block: ((_ row: Row) throws -> ())) throws {
        let sql: String
        if let test = test {
            sql = "SELECT \(cols.joined(separator: ",")) FROM \(table) WHERE \(test)"
        } else {
            sql = "SELECT \(cols.joined(separator: ",")) FROM \(table)"
        }
        try prepare(sql: sql).results(block)
    }

    public func select(_ cols: [Column], from table: String, where test: String? = nil, _ block: ([Any?]) -> Void)
    throws {
        try select(cols.map(\.name), from: table, where: test) { row in
            block(row.values(for: cols))
        }
    }
}

extension Row {
    
    public func anyValues() throws -> [Any?] {
        var values = [Any?]()
        for ndx in 0..<count {
            values.append(try self.value(at: ndx).anyValue)
        }
        return values
    }
    
    public func values(for columns:[Column]) -> [Any?] {
        var values = [Any?]()
        for (ndx, c) in columns.enumerated() {
            let v = try? c.read(value(at: ndx))
            values.append(v)
        }
        return values
    }
}

public extension String {

    func sql ( with values: [Any]) throws -> String {
        var out = ""
        
        var has_format = false
        var ndx: Int = 0
        
        for ch in self {
            if has_format && ch == "%" {
                print("%%", terminator: "", to: &out)
                has_format = false
                continue
            }
            if ch == "%" { has_format = true; continue }
            if has_format {
                guard ndx < values.count else { throw SQLFormatError.tooFewParameters }
                var value = values[ndx]
                ndx += 1
                let num_v = (value as? NSNumber) // ?? NSNumber(value: 0)
                let str_v = value as? String
                let bool_v = value as? Bool
                
                switch ch {
                case "b":
                    value = bool_v ?? num_v?.boolValue ?? "NULL"
                case "f":
                    value = num_v?.floatValue ?? "NULL"
                case "i":
                    value = num_v?.intValue ?? "NULL"
                case "d":
                    value = num_v?.doubleValue ?? "NULL"
                case "K": // column name
                    value = str_v ?? "NULL"
                case "T": // table name
                    value = str_v ?? "NULL"
                case "@": // string to be quoted
                    if let str = str_v {
                        value = "'\(str)'"
                    } else {
                        value = "NULL"
                    }
                case "J": // path to json_extract
                    if let str = str_v {
                        value = Database.format(column: str, as: nil)
                    } else {
                        value = "NULL"
                    }
                default:
                    throw SQLFormatError.unrecognizedOption
                }
                print(value, terminator: "", to: &out)
                has_format = false
            } else {
                print(ch, terminator: "", to: &out)
            }
        }
        if ndx != values.count { throw SQLFormatError.tooManyParameters }
        return out
    }
}


extension Array: DatabaseSerializable {
    
    public func serialized() -> DatabaseValue {
        guard let data = try? JSONSerialization.data(withJSONObject: self, options: []),
              let str = String(data: data, encoding: .utf8)
        else { return .null }
        return .text(str)
    }
    
    public static func deserialize(from value: DatabaseValue) throws -> Array<Element> {
        guard case let DatabaseValue.text(str) = value,
              let data = str.data(using: .utf8)
        else { throw DatabaseError("Cannot deserialize \(value) into Array") }
        guard let results = try JSONSerialization.jsonObject(with: data, options: []) as? Self
        else { throw DatabaseError("Cannot deserialize \(value) into Array") }
        return results
    }
}

extension Dictionary: DatabaseSerializable {

    /// The binding value representation of the type to be bound to a `Statement`.
    public func serialized() -> DatabaseValue {
        guard let data = try? JSONSerialization.data(withJSONObject: self, options: []),
              let str = String(data: data, encoding: .utf8)
            else { return .null }  // .text("") }
        return .text(str)
    }

    /// Converts the binding value `Any` object representation to an equivalent `Date` representation.
    public static func deserialize(from value: DatabaseValue) throws -> Self {
        guard case let DatabaseValue.text(str) = value,
              let data = str.data(using: .utf8)
        else {
            throw DatabaseError("Cannot deserialize \(value) into Dictionary") }
        guard let results = try JSONSerialization.jsonObject(with: data, options: []) as? Self
        else {
            throw DatabaseError("Cannot deserialize \(value) into Dictionary") }
        return results
    }
}

extension Database {
    
    public func count(_ table: String, where test: String? = nil) -> Int {
        var sql: String
        if let test = test {
            sql = "SELECT count(*) FROM \(table) WHERE \(test)"
        } else {
            sql = "SELECT count(*) FROM \(table)"
        }
        var count: Int?
        try? results(sql: sql) { row in
            if let value = row.first,
                case let DatabaseValue.integer(int) = value {
                count = Int(int)
            }
        }
        return count ?? 0
    }

    public func create(view: String, from table: String, select cols: [String]) throws {

        let formatted_cols = cols.map { format(column: $0, as: $0) }
        let sql = """
            CREATE VIEW \(table) IF NOT EXISTS AS SELECT \(formatted_cols) from \(table)
        """

        try execute(sql: sql)
    }

    public func create(_ table: String, addID: Bool = true, with defs: String) throws {

        let sql = addID
            ? "CREATE TABLE \(table) (id INTEGER PRIMARY KEY, \(defs))"
            : "CREATE TABLE \(table) (\(defs))"

        try execute(sql: "DROP TABLE IF EXISTS \(table)")
        try execute(sql: sql)
    }

    /// Formats json keypaths for SQL query
    public func format(column: String, as alias: String? = nil) -> String {
        Self.format(column: column, as: alias)
    }
    
    static public func format(column: String, as alias: String? = nil) -> String {
        guard column.starts(with: "$") ||  column.contains(".")
            else { return column }
        let parts = column.starts(with: "$")
            ? column.dropFirst().split(separator: ".", maxSplits: 1)
            : column.split(separator: ".", maxSplits: 1)

        let str = "json_extract(\(parts[0]),'$.\(parts[1])')"

        let alias = alias ?? column.replacingOccurrences(of: ".", with: "_")
        return "\(str) AS \(alias)"
    }

    /// This method is responsible for inserting the indicated Dictionary items into
    /// the database.
    public func load(json: NSObject, from key: String? = nil, into table: String) throws {

        var plist: Any?

        if let key = key, !key.isEmpty {
            plist = json.value(forKeyPath: key)
        } else {
            plist = json
        }
        guard let items = plist as? [Any] else {
            throw DatabaseError("Expected JSON Array") }
        
        for item in items {
            guard let dict = item as? [String:ParameterBindable] else { continue }
            try insert(into: table, from: dict)
        }
    }

    public func insert(_ columns: [String], into table: String, values: [[ParameterBindable]]) throws {
        
        let slots = Array<String>(repeating: "?", count: columns.count)

        let sql = "INSERT INTO \(table) (\(columns.joined(separator: ","))) VALUES(\(slots.joined(separator: ",")))"
        try transaction { db in
            for row in values {
                try db.execute(sql: sql, parameterValues: row)
            }
            return .commit
        }
    }

    /// This `insert` method is useful when it is desirable to insert  values
    /// from a Dictionary.
    ///
    /// - Parameter table: The name of the table
    ///
    /// - Parameter plist: A Dictionary with values for a record.
    ///
    /// - Returns: Void
    ///
    /// - Throws: A `SQLiteError` if SQLite encounters an error stepping through the statement.
    @discardableResult
    public func insert(into table: String, from plist: [String:ParameterBindable]) throws -> Int64 {
        
        var keys: [String] = []
        var slots: [String] = []
        var values: [ParameterBindable?] = []
        
        for (key, val) in plist {
            keys.append(key)
            slots.append("?")
            values.append(val)
        }
        let sql = "INSERT INTO \(table) (\(keys.joined(separator: ","))) VALUES(\(slots.joined(separator: ",")))"
        
        try execute(sql: sql, parameterValues: values)
        return self.lastInsertRowid ?? 0
    }

    /// The `delete` method deletes records from the given `table`
    /// In the exceptional case you really want to remove all the records
    /// in the table the `confirmAll` MUST be explicitly set and the test
    /// be empty
    public func delete(from table: String, where test: String, confirmAll: Bool = false) throws {
        if confirmAll, test == "" {
            try execute(sql: "DELETE FROM \(table)")
        } else {
            try execute(sql: "DELETE FROM \(table) WHERE \(test)")
        }
    }

    /// This `update` method is useful when it is desirable to update  values
    /// from a Dictionary. Furthermore the row is only updated if the values
    /// have actually changed thus avoiding any trigger when nothing has changed.
    ///
    /// - Parameter table: The name of the table
    ///
    /// - Parameter plist: A Dictionary with values for a record.
    /// - Parameter limit: A negative limit indicates ALL or NO Limit
    ///
    /// - Returns: Void
    ///
    /// - Throws: A `SQLiteError` if SQLite encounters an error stepping through the statement.
    public func update(table: String, with plist: [String:ParameterBindable], limit: Int = -1) throws {
        
            var sets: [String] = []
            var values: [ParameterBindable?] = []
        
            for (key, value) in plist {
                sets.append("\(key) = ?")
                values.append(value)
            }
            
        let sql = """
                UPDATE \(table)
                SET \(sets.joined(separator: ","))
                WHERE NOT (
                    \(sets.joined(separator: " AND ")))
                LIMIT \(limit)
            """
            /*
             ORDER column_or_expression
             LIMIT row_count OFFSET offset;
             */
            try execute(sql: sql, parameterValues: values)
    }
}
