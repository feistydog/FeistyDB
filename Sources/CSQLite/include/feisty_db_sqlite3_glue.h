//
// Copyright (c) 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

#include "sqlite3.h"

// Wrappers for some C variadic functions used in SQLite to make them accessible from Swift

/// Duplicates and returns `s` using memory allocated by `sqlite3_malloc()`
char * feisty_db_sqlite3_strdup(const char *s);

/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, x, y)`
int feisty_db_sqlite3_db_config_enable_fkey(sqlite3 *db, int x, int *y);
/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, x, y)`
int feisty_db_sqlite3_db_config_enable_trigger(sqlite3 *db, int x, int *y);
/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_VIEW, x, y)`
int feisty_db_sqlite3_db_config_enable_view(sqlite3 *db, int x, int *y);
/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, x, y)`
int feisty_db_sqlite3_db_config_enable_load_extension(sqlite3 *db, int x, int *y);
/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, x, y)`
int feisty_db_sqlite3_db_config_no_ckpt_on_close(sqlite3 *db, int x, int *y);
/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_QPSG, x, y)`
int feisty_db_sqlite3_db_config_enable_qpsg(sqlite3 *db, int x, int *y);
/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_DEFENSIVE, x, y)`
int feisty_db_sqlite3_db_config_defensive(sqlite3 *db, int x, int *y);
/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_WRITABLE_SCHEMA, x, y)`
int feisty_db_sqlite3_db_config_writable_schema(sqlite3 *db, int x, int *y);
/// Equivalent to `sqlite3_db_config(db, SQLITE_DBCONFIG_TRUSTED_SCHEMA, x, y)`
int feisty_db_sqlite3_db_config_trusted_schema(sqlite3 *db, int x, int *y);

/// Equivalent to `sqlite3_vtab_config(db, SQLITE_VTAB_CONSTRAINT_SUPPORT, x)`
int feisty_db_sqlite3_vtab_config_constraint_support(sqlite3 *db, int x);
/// Equivalent to `sqlite3_vtab_config(db, SQLITE_VTAB_INNOCUOUS)`
int feisty_db_sqlite3_vtab_config_innocuous(sqlite3 *db);
/// Equivalent to `sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY)`
int feisty_db_sqlite3_vtab_config_directonly(sqlite3 *db);

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
