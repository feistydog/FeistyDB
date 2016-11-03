/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// Possible transaction types
///
/// - seealso: [Transactions in SQLite](https://sqlite.org/lang_transaction.html)
public enum TransactionType: CustomStringConvertible {
	/// A deferred transaction
	case deferred
	/// An immediate transaction
	case immediate
	/// An exclusive transaction
	case exclusive

	/// A description of the transaction type suitable for use in an SQL statement
	public var description: String {
		switch self {
		case .deferred:		return "DEFERRED"
		case .immediate:	return "IMMEDIATE"
		case .exclusive:	return "EXCLUSIVE"
		}
	}
}
