//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

import Foundation
import CSQLite

/// An error supplying a message and description.
public protocol Error: Swift.Error {
	/// A brief message describing the error
	var message: String { get }

	/// A more detailed description of the error's cause
	var details: String? { get }
}

/// An error supplying a message and description.
public struct DatabaseError: Error {
	/// A brief message describing the error
	public let message: String

	/// A more detailed description of the error's cause
	public let details: String?
}

extension DatabaseError {
	/// Creates an error with the given message.
	///
	/// - parameter message: A brief message describing the error
	public init(_ message: String) {
		self.message = message
		self.details = nil
	}
}

extension DatabaseError: CustomStringConvertible {
	public var description: String {
		if let details = details {
			return "\(message): \(details)"
		}
		else {
			return message
		}
	}
}

extension DatabaseError: LocalizedError {
	public var errorDescription: String? {
		return message
	}

	public var failureReason: String? {
		return details
	}
}
