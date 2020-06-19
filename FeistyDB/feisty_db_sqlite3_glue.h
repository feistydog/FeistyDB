//
// Copyright (c) 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

#include "sqlite3.h"

// Wrappers for some C variadic functions used in SQLite to make them accessible from Swift

/// Duplicates and returns `s` using memory allocated by `sqlite3_malloc()`
char * feisty_db_sqlite3_strdup(const char *s);

/// Equivalent to `sqlite3_vtab_config(d, SQLITE_VTAB_CONSTRAINT_SUPPORT, x)`
int feisty_db_sqlite3_vtab_config_constraint_support(sqlite3 *d, int x);
/// Equivalent to `sqlite3_vtab_config(d, SQLITE_VTAB_INNOCUOUS)`
int feisty_db_sqlite3_vtab_config_innocuous(sqlite3 *d);
/// Equivalent to `sqlite3_vtab_config(d, SQLITE_VTAB_DIRECTONLY)`
int feisty_db_sqlite3_vtab_config_directonly(sqlite3 *d);
