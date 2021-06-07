//
// Copyright (c) 2020 - 2021 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

#include "sqlite3.h"

// Structures used in both C and Swift are defined here to ensure the correct memory layout
// See https://lists.swift.org/pipermail/swift-users/Week-of-Mon-20160516/001968.html

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
