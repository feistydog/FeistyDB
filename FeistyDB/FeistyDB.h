//
// Copyright (c) 2015 - 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

@import Foundation;

//! Project version number for FeistyDB.
FOUNDATION_EXPORT double FeistyDBVersionNumber;

//! Project version string for FeistyDB.
FOUNDATION_EXPORT const unsigned char FeistyDBVersionString[];

#include "sqlite3.h"
#include "sqlite3ext.h"

struct feisty_db_sqlite3_vtab {
	/// sqlite3 required fields
	sqlite3_vtab base;
	/// `UnsafeMutablePointer<VirtualTableModule>`
	void *virtual_table_module_ptr;
};
typedef struct feisty_db_sqlite3_vtab feisty_db_sqlite3_vtab;

struct feisty_db_sqlite3_vtab_cursor {
	/// sqlite3 required fields
	sqlite3_vtab_cursor base;
	/// `UnsafeMutablePointer<VirtualTableCursor>`
	void *virtual_table_cursor_ptr;
};
typedef struct feisty_db_sqlite3_vtab_cursor feisty_db_sqlite3_vtab_cursor;
