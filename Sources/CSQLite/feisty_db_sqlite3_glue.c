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


int feisty_db_sqlite3_db_config_enable_fkey(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, x, y);
}

int feisty_db_sqlite3_db_config_enable_trigger(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, x, y);
}

int feisty_db_sqlite3_db_config_enable_view(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_VIEW, x, y);
}

int feisty_db_sqlite3_db_config_enable_load_extension(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, x, y);
}

int feisty_db_sqlite3_db_config_no_ckpt_on_close(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, x, y);
}

int feisty_db_sqlite3_db_config_enable_qpsg(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_QPSG, x, y);
}

int feisty_db_sqlite3_db_config_defensive(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_DEFENSIVE, x, y);
}

int feisty_db_sqlite3_db_config_writable_schema(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_WRITABLE_SCHEMA, x, y);
}

int feisty_db_sqlite3_db_config_legacy_alter_table(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_LEGACY_ALTER_TABLE, x, y);
}

int feisty_db_sqlite3_db_config_dqs_dml(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_DQS_DML, x, y);
}

int feisty_db_sqlite3_db_config_dqs_ddl(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_DQS_DDL, x, y);
}

int feisty_db_sqlite3_db_config_trusted_schema(sqlite3 *db, int x, int *y)
{
	return sqlite3_db_config(db, SQLITE_DBCONFIG_TRUSTED_SCHEMA, x, y);
}


int feisty_db_sqlite3_vtab_config_constraint_support(sqlite3 *db, int x)
{
	return sqlite3_vtab_config(db, SQLITE_VTAB_CONSTRAINT_SUPPORT, x);
}

int feisty_db_sqlite3_vtab_config_innocuous(sqlite3 *db)
{
	return sqlite3_vtab_config(db, SQLITE_VTAB_INNOCUOUS);
}

int feisty_db_sqlite3_vtab_config_directonly(sqlite3 *db)
{
	return sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
}
