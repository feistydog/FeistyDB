//
// Copyright (c) 2015 - 2018 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

#include "sqlite3.h"

// Bogus prototype
void sqlite3_uuid_init(void);

__attribute__((constructor))
static void feisty_db_register_sqlite_uuid_extension()
{
	sqlite3_auto_extension(sqlite3_uuid_init);
}
