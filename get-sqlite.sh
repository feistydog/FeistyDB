#!/bin/sh

SQLITE_ARCHIVE=sqlite-src-3340000.zip
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

/usr/bin/unzip -u "./$SQLITE_ARCHIVE"

if ! [ -f "./$SQLITE_DIR/Makefile" ]; then
	(cd "./$SQLITE_DIR" && ./configure --disable-tcl)
fi

/usr/bin/make -C "./$SQLITE_DIR" sqlite3.c

/usr/bin/fgrep "FeistyDB" "./$SQLITE_DIR/sqlite3.c" > /dev/null
STATUS=$?

if [ $STATUS -gt 1 ]; then
	echo "Error: fgrep failed"
	exit 1
elif [ $STATUS -eq 1 ]; then
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************************** FeistyDB additions ****************************/

/*
** Include the uuid and carray sqlite extensions.
**
** To omit the uuid extension define FEISTY_DB_OMIT_UUID.
** To omit the carray extension define FEISTY_DB_OMIT_CARRAY.
**
** To automatically make the extensions available to every sqlite
** connection, add "-DSQLITE_EXTRA_INIT=feisty_db_init" to this
** file's Compiler Flags in the Build Phases tab in Xcode
**
*/

#ifndef FEISTY_DB_OMIT_UUID
EOF
	cat "$SQLITE_DIR/ext/misc/uuid.c" >> "$SQLITE_DIR/sqlite3.c"
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
#endif /* FEISTY_DB_OMIT_UUID */

#ifndef FEISTY_DB_OMIT_CARRAY
EOF
	cat "$SQLITE_DIR/ext/misc/carray.c" >> "$SQLITE_DIR/sqlite3.c"
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
#endif /* FEISTY_DB_OMIT_CARRAY */

int feisty_db_init(const char *dummy)
{
	int nErr = 0;

#ifndef FEISTY_DB_OMIT_UUID
	nErr += sqlite3_auto_extension((void *)sqlite3_uuid_init);
#endif /* FEISTY_DB_OMIT_UUID */

#ifndef FEISTY_DB_OMIT_CARRAY
	nErr += sqlite3_auto_extension((void *)sqlite3_carray_init);
#endif /* FEISTY_DB_OMIT_CARRAY */

	return nErr ? SQLITE_ERROR : SQLITE_OK;
}

/************************** End of FeistyDB additions *********************/
EOF
fi

/bin/mv "$SQLITE_DIR/sqlite3.c" "./Sources/CSQLite/"
/bin/mv "$SQLITE_DIR/sqlite3.h" "./Sources/CSQLite/include/"
/bin/mv "$SQLITE_DIR/sqlite3ext.h" "./Sources/CSQLite/include/"

echo "sqlite successfully configured for FeistyDB"
