/*
 *  Copyright (C) 2015, 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

import Foundation

/// Possible errors
public enum DatabaseError : Error {
	/// An error from the underlying SQLite library
	case sqliteError(String)
	/// An error indicating data was encountered in an unexpected or illegal format
	case dataFormatError(String)
}
