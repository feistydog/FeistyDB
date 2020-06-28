//
//  SQLIteCoder.swift
//  FeistyDB
//
//  Created by Jason Jobe on 6/26/20.
//

import Foundation


public class SQLiteDecoder: Decoder {
    
    enum DecodeError: Swift.Error { case bad(String) }
    
    public var row: Row
    public var codingPath: [CodingKey] = []
    public var userInfo: [CodingUserInfoKey : Any] = [:]

    public init(row: Row) {
        self.row = row
    }
    
    public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
        return KeyedDecodingContainer(KeyedContainer<Key>(decoder: self, codingPath: codingPath, userInfo: userInfo))
    }
    
    public func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw DecodeError.bad(#function)
    }
    
    public func singleValueContainer() throws -> SingleValueDecodingContainer {
        throw DecodeError.bad(#function)
    }
}

//func trace(_ line: Int, _ f: String, _ key: CodingKey) {
//    Swift.print(line, f, key)
//}

extension DatabaseValue {
    var isNull: Bool { self == .null }
    func boolValue() throws -> Bool {
        switch self {
            case .blob(_): return false
            case .float(_): return false
            case .integer(let i): return i != 0
            case .text(let s): return s == "true"
            default:
                return false
        }
    }
}

extension SQLiteDecoder {
    final class KeyedContainer<Key> where Key: CodingKey {
        enum DecodeError: Swift.Error { case bad(String) }
        
        var decoder: SQLiteDecoder
        var codingPath: [CodingKey] //{ return [] }
        var allKeys: [Key] { return [] }
        var userInfo: [CodingUserInfoKey : Any] = [:]
        
        init(decoder: SQLiteDecoder, codingPath: [CodingKey], userInfo: [CodingUserInfoKey : Any]) {
            self.decoder = decoder
            self.codingPath = codingPath
            self.userInfo = userInfo
        }
    }
}

extension SQLiteDecoder.KeyedContainer: KeyedDecodingContainerProtocol {
    
    func contains(_ key: Key) -> Bool {
        let value = try? decoder.row.value(named: key.stringValue)
        return value?.isNull ?? true
    }
    
    func decodeNil(forKey key: Key) throws -> Bool {
        let value = try? decoder.row.value(named: key.stringValue)
        return value?.isNull ?? true
    }
    
    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
        let value = try decoder.row.value(named: key.stringValue)
        return try value.boolValue()
    }
    
    func decode(_ type: String.Type, forKey key: Key) throws -> String {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {        return try decoder.row.value(named: key.stringValue)

    }
    
    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
        return try decoder.row.value(named: key.stringValue)
    }
    
    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
        return try type.init(from: decoder)
    }
    
    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        return try decoder.container(keyedBy: type)
    }
    
    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw DecodeError.bad(#function)
    }
    
    func superDecoder() throws -> Decoder {
        decoder
    }
    
    func superDecoder(forKey key: Key) throws -> Decoder {
        decoder
    }
}
