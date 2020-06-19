//
// Copyright (c) 2020 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

#include "feisty_db_sqlite3_glue.h"

char * feisty_db_sqlite3_strdup(const char *s)
{
	return sqlite3_mprintf("%s", s);
}

int feisty_db_sqlite3_vtab_config_constraint_support(sqlite3 *d, int x)
{
	return sqlite3_vtab_config(d, SQLITE_VTAB_CONSTRAINT_SUPPORT, x);
}

int feisty_db_sqlite3_vtab_config_innocuous(sqlite3 *d)
{
	return sqlite3_vtab_config(d, SQLITE_VTAB_INNOCUOUS);
}

int feisty_db_sqlite3_vtab_config_directonly(sqlite3 *d)
{
	return sqlite3_vtab_config(d, SQLITE_VTAB_DIRECTONLY);
}
