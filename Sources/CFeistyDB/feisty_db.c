//
// Copyright (c) 2021 Feisty Dog, LLC
//
// See https://github.com/feistydog/FeistyDB/blob/master/LICENSE.txt for license information
//

#include "csqlite_shims.h"

static void feisty_db_initialize(void) __attribute__ ((constructor));
static void feisty_db_initialize()
{
	// It's necessary to call sqlite3_initialize() since SQLITE_OMIT_AUTOINIT is defined in CSQLite
	sqlite3_initialize();
	csqlite_sqlite3_auto_extension_carray();
	csqlite_sqlite3_auto_extension_decimal();
	csqlite_sqlite3_auto_extension_ieee754();
	csqlite_sqlite3_auto_extension_series();
	csqlite_sqlite3_auto_extension_sha3();
	csqlite_sqlite3_auto_extension_uuid();
}
