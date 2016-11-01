/*
 *  Copyright (C) 2016 Feisty Dog, LLC
 *  All Rights Reserved
 */

#include "sqlite3.h"

// Bogus prototype
void sqlite3_uuid_init();

__attribute__((constructor))
static void feisty_db_register_sqlite_uuid_extension()
{
	sqlite3_auto_extension(sqlite3_uuid_init);
}
