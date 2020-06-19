//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import os.log
import Foundation

// Protocol declarations can't be nested, otherwise this would be inside Database

/// An interface to a custom FTS5 tokenizer.
public protocol FTS5Tokenizer {
	/// Initializes an FTS5 tokenizer.
	///
	/// - parameter arguments: The tokenizer arguments used to create the FTS5 table.
	init(arguments: [String])

	/// Sets the text to be tokenized.
	///
	/// - parameter text: The text to be tokenized.
	/// - parameter reason: The reason tokenization is being requested.
	func setText(_ text: String, reason: Database.FTS5TokenizationReason)

	/// Advances the tokenizer to the next token.
	///
	/// - returns: `true` if a token was found, `false` otherwise
	func advance() -> Bool

	/// Returns the current token.
	///
	/// - returns: The current token or `nil` if none
	func currentToken() -> String?

	/// Copies the current token in UTF-8 to the supplied buffer.
	///
	/// - parameter buffer: A buffer to receive the current token encoded in UTF-8
	/// - parameter capacity: The number of bytes availabe in `buffer`
	///
	/// - throws: An error if `buffer` has insufficient capacity for the token
	///
	/// - returns: The number of bytes written to `buffer`
	func copyCurrentToken(to buffer: UnsafeMutablePointer<UInt8>, capacity: Int) throws -> Int
}

extension Database {
	/// Glue for creating a generic Swift type in a C callback
	final class FTS5TokenizerCreator {
		/// The constructor closure
		let construct: (_ arguments : [String]) throws -> FTS5Tokenizer

		/// Creates a new FTS5TokenizerCreator.
		///
		/// - parameter construct: A closure that creates the tokenizer
		init(_ construct: @escaping (_ arguments: [String]) -> FTS5Tokenizer)
		{
			self.construct = construct
		}
	}

	/// The reasons FTS5 will request tokenization
	public enum FTS5TokenizationReason {
		/// A document is being inserted into or removed from the FTS table
		case document
		/// A `MATCH` query is being executed against the FTS index
		case query
		/// Same as `query`, except that the bareword or quoted string is followed by a `*` character
		case prefix
		/// The tokenizer is being invoked to satisfy an `fts5_api.xTokenize()` request made by an auxiliary function
		case aux
	}

	/// Adds a custom FTS5 tokenizer.
	///
	/// For example, a word tokenizer using CFStringTokenizer could be implemented as:
	/// ```swift
	/// class WordTokenizer: FTS5Tokenizer {
	/// 	var tokenizer: CFStringTokenizer!
	/// 	var text: CFString!
	///
	/// 	required init(arguments: [String]) {
	/// 		// Arguments not used
	/// 	}
	///
	/// 	func set(text: String, reason: Database.FTS5TokenizationReason) {
	/// 		// Reason not used
	/// 		self.text = text as CFString
	/// 		tokenizer = CFStringTokenizerCreate(kCFAllocatorDefault, self.text, CFRangeMake(0, CFStringGetLength(self.text)), kCFStringTokenizerUnitWord, nil)
	/// 	}
	///
	/// 	func advance() -> Bool {
	/// 		let nextToken = CFStringTokenizerAdvanceToNextToken(tokenizer)
	/// 		guard nextToken != CFStringTokenizerTokenType(rawValue: 0) else {
	/// 			return false
	/// 		}
	/// 		return true
	/// 	}
	///
	/// 	func currentToken() -> String? {
	/// 		let tokenRange = CFStringTokenizerGetCurrentTokenRange(tokenizer)
	/// 		guard tokenRange.location != kCFNotFound /*|| tokenRange.length != 0*/ else {
	/// 			return nil
	/// 		}
	/// 		return CFStringCreateWithSubstring(kCFAllocatorDefault, text, tokenRange) as String
	/// 	}
	///
	/// 	func copyCurrentToken(to buffer: UnsafeMutablePointer<UInt8>, capacity: Int) throws -> Int {
	/// 		let tokenRange = CFStringTokenizerGetCurrentTokenRange(tokenizer)
	/// 		var bytesConverted = 0
	/// 		let charsConverted = CFStringGetBytes(text, tokenRange, CFStringBuiltInEncodings.UTF8.rawValue, 0, false, buffer, capacity, &bytesConverted)
	/// 		guard charsConverted > 0 else {
	/// 			throw DatabaseError("Insufficient buffer size")
	/// 		}
	/// 		return bytesConverted
	/// 	}
	/// }
	/// ```
	///
	/// - parameter name: The name of the tokenizer
	/// - parameter type: The class implementing the tokenizer
	///
	/// - throws:  An error if the tokenizer can't be added
	///
	/// - seealso: [Custom Tokenizers](https://www.sqlite.org/fts5.html#custom_tokenizers)
	public func addTokenizer<T: FTS5Tokenizer>(_ name: String, type: T.Type) throws {
		// Fail early if FTS5 isn't available
		let api_ptr = try get_fts5_api(for: db)

		// Flesh out the struct containing the xCreate, xDelete, and xTokenize functions used by SQLite
		var tokenizer_struct = fts5_tokenizer(xCreate: { (user_data, argv, argc, out) -> Int32 in
			// Create the tokenizer instance using the creation function passed to fts5_api.xCreateTokenizer()
			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { String(utf8String: $0.unsafelyUnwrapped).unsafelyUnwrapped }

			let tokenizer: FTS5Tokenizer
			do {
				let creator = Unmanaged<FTS5TokenizerCreator>.fromOpaque(UnsafeRawPointer(user_data.unsafelyUnwrapped)).takeUnretainedValue()
				tokenizer = try creator.construct(arguments)
			}

			catch let error {
				os_log("Error constructing FTS5 tokenizer: %{public}@", type: .info, error.localizedDescription)
				return SQLITE_ERROR
			}

			// tokenizer must live until the xDelete function is invoked; store it as a +1 object in ptr
			let ptr = Unmanaged.passRetained(tokenizer as AnyObject).toOpaque()
			out?.initialize(to: OpaquePointer(ptr))

			return SQLITE_OK
		}, xDelete: { p in
			// Balance the +1 retain above
			Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(p.unsafelyUnwrapped)).release()
		}, xTokenize: { (tokenizer_ptr, context, flags, text_utf8, text_len, xToken) -> Int32 in
			// Tokenize the text and invoke xToken for each token found
			let tokenizer = Unmanaged<AnyObject>.fromOpaque(UnsafeRawPointer(tokenizer_ptr.unsafelyUnwrapped)).takeUnretainedValue() as! FTS5Tokenizer

			// Set the text to be tokenized
			let text = String(bytesNoCopy: UnsafeMutableRawPointer(mutating: text_utf8.unsafelyUnwrapped), length: Int(text_len), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
			let reason = FTS5TokenizationReason(flags)

			tokenizer.setText(text, reason: reason)

			// Use a local buffer for token extraction if possible
			let bufsize = 512
			var buf = UnsafeMutablePointer<UInt8>.allocate(capacity: bufsize)

			defer {
				buf.deallocate()
			}

			// Process each token and pass to FTS5
			while tokenizer.advance() {
				do {
					// Attempt to copy the current token to buf
					let byteCount = try tokenizer.copyCurrentToken(to: buf, capacity: bufsize)

					let result = UnsafePointer(buf).withMemoryRebound(to: Int8.self, capacity: bufsize) { bytes in
						return xToken.unsafelyUnwrapped(context, 0, bytes, Int32(byteCount), 0, Int32(byteCount))
					}

					guard result == SQLITE_OK else {
						return result
					}
				}

				catch {
					// The token was too large to fit in buf
					guard let token = tokenizer.currentToken() else {
						continue
					}
					let utf8 = token.utf8
					let result = xToken.unsafelyUnwrapped(context, 0, token, Int32(utf8.count), 0, Int32(utf8.count))
					guard result == SQLITE_OK else {
						return result
					}
				}
			}

			return SQLITE_OK
		})

		// user_data must live until the xDestroy function is invoked; store it as a +1 object
		let user_data = FTS5TokenizerCreator { (args) -> FTS5Tokenizer in
			return T(arguments: args)
		}
		let user_data_ptr = Unmanaged.passRetained(user_data).toOpaque()

		guard api_ptr.pointee.xCreateTokenizer(UnsafeMutablePointer(mutating: api_ptr), name, user_data_ptr, &tokenizer_struct, { user_data in
			// Balance the +1 retain above
			Unmanaged<FTS5TokenizerCreator>.fromOpaque(UnsafeRawPointer(user_data.unsafelyUnwrapped)).release()
		}) == SQLITE_OK else {
			// xDestroy is not called if fts5_api.xCreateTokenizer() fails
			Unmanaged<FTS5TokenizerCreator>.fromOpaque(user_data_ptr).release()
			throw SQLiteError("Error creating FTS5 tokenizer", takingDescriptionFromDatabase: db)
		}
	}
}

extension Database.FTS5TokenizationReason {
	/// Convenience initializer for conversion of `FTS5_TOKENIZE_` values
	///
	/// - parameter flags: The flags passed as the second argument of `fts5_tokenizer.xTokenize()`
	init(_ flags: Int32) {
		switch flags {
		case FTS5_TOKENIZE_DOCUMENT: 						self = .document
		case FTS5_TOKENIZE_QUERY: 							self = .query
		case FTS5_TOKENIZE_QUERY | FTS5_TOKENIZE_PREFIX: 	self = .prefix
		case FTS5_TOKENIZE_AUX: 							self = .aux
		default:											preconditionFailure("Unexpected FTS5 flag")
		}
	}
}

/// Returns a pointer to the `fts5_api` structure for `db`.
///
/// - parameter db: The database connection to query
///
/// - throws:  An error if the `fts5_api` structure couldn't be retrieved
///
/// - returns: A pointer to the global `fts5_api` structure for `db`
func get_fts5_api(for db: SQLiteDatabaseConnection) throws -> UnsafePointer<fts5_api> {
	var stmt: SQLitePreparedStatement? = nil
	let sql = "SELECT fts5(?1);"
	guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
		throw SQLiteError("Error preparing SQL \"\(sql)\"", takingDescriptionFromDatabase: db)
	}

	defer {
		sqlite3_finalize(stmt)
	}

	var api_ptr: UnsafePointer<fts5_api>?
	guard sqlite3_bind_pointer(stmt, 1, &api_ptr, "fts5_api_ptr", nil) == SQLITE_OK else {
		throw SQLiteError("Error binding FTS5 API pointer", takingDescriptionFromStatement: stmt!)
	}

	guard sqlite3_step(stmt) == SQLITE_ROW else {
		throw SQLiteError("Error retrieving FTS5 API pointer", takingDescriptionFromStatement: stmt!)
	}

	guard let api = api_ptr else {
		throw DatabaseError("FTS5 not available")
	}

	return api
}
