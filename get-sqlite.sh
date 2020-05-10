#!/bin/sh

SQLITE_ARCHIVE=sqlite-src-3310100.zip
SQLITE_DOWNLOAD_URL=https://sqlite.org/2020/$SQLITE_ARCHIVE
SQLITE_DIR=$(basename "$SQLITE_ARCHIVE" .zip)

if ! [ -f "./$SQLITE_ARCHIVE" ]; then
	if ! [ -x "$(command -v curl)" ]; then
		echo "Error: $SQLITE_ARCHIVE not found and curl not present"
		echo "Please download sqlite manually from"
		echo "$SQLITE_DOWNLOAD_URL and re-run this script"
		exit 1
	fi

	curl -O "$SQLITE_DOWNLOAD_URL"
fi

/usr/bin/unzip -u "$SQLITE_ARCHIVE"

(cd "./$SQLITE_DIR" && ./configure --disable-tcl && make sqlite3.c)

MATCH_FOUND=$(/usr/bin/fgrep "FeistyDB" "$SQLITE_DIR/sqlite3.c")

if test "$MATCH_FOUND" != 0; then
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************************** FeistyDB additions ****************************/

/*
** Include the uuid and carray sqlite extensions.
**
** To omit the uuid extension define FEISTYDB_OMIT_UUID.
** To omit the carray extension define FEISTYDB_OMIT_CARRAY.
**
** To automatically make the extensions available to every sqlite
** connection, add "-DSQLITE_EXTRA_INIT=feisty_db_init" to this
** file's Compiler Flags in the Build Phases tab in Xcode
**
*/

#ifndef FEISTYDB_OMIT_UUID
#include "ext/misc/uuid.c"
#endif

#ifndef FEISTYDB_OMIT_CARRAY
#include "ext/misc/carray.c"
#endif

int feisty_db_init(const char *dummy)
{
	int nErr = 0;

#ifndef FEISTYDB_OMIT_UUID
	nErr += sqlite3_auto_extension((void *)sqlite3_uuid_init);
#endif

#ifndef FEISTYDB_OMIT_CARRAY
	nErr += sqlite3_auto_extension((void *)sqlite3_carray_init);
#endif

	return nErr ? SQLITE_ERROR : SQLITE_OK;
}

/************************** End of FeistyDB additions *********************/

EOF
fi

if [ -d ./sqlite ]; then
	/bin/rm ./sqlite
fi

ln -s "$SQLITE_DIR" sqlite
