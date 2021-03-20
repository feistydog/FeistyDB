#!/bin/sh

SQLITE_ARCHIVE=sqlite-src-3350200.zip
SQLITE_DOWNLOAD_URL=https://sqlite.org/2021/$SQLITE_ARCHIVE
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
** Include various sqlite loadable extensions.
**
** To automatically make the extensions available to every sqlite
** connection, add "-DSQLITE_EXTRA_INIT=feisty_db_init" to this
** file's Compiler Flags in the Build Phases tab in Xcode
**
*/

EOF

	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************** Begin file carray.c ******************************************/
EOF
	cat "$SQLITE_DIR/ext/misc/carray.c" >> "$SQLITE_DIR/sqlite3.c"
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************** End of carray.c **********************************************/
/************** Begin file decimal.c *****************************************/
EOF
	cat "$SQLITE_DIR/ext/misc/decimal.c" >> "$SQLITE_DIR/sqlite3.c"
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************** End of decimal.c *********************************************/
/************** Begin file ieee754.c *****************************************/
EOF
	cat "$SQLITE_DIR/ext/misc/ieee754.c" >> "$SQLITE_DIR/sqlite3.c"
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************** End of ieee754.c *********************************************/
/************** Begin file series.c ******************************************/
EOF
	cat "$SQLITE_DIR/ext/misc/series.c" >> "$SQLITE_DIR/sqlite3.c"
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************** End of series.c **********************************************/
/************** Begin file shathree.c ****************************************/
EOF
	cat "$SQLITE_DIR/ext/misc/shathree.c" >> "$SQLITE_DIR/sqlite3.c"
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************** End of shathree.c ********************************************/
/************** Begin file uuid.c ********************************************/
EOF
	cat "$SQLITE_DIR/ext/misc/uuid.c" >> "$SQLITE_DIR/sqlite3.c"
	cat <<EOF >> "$SQLITE_DIR/sqlite3.c"
/************** End of uuid.c ************************************************/

/*
** Automatically make the extensions available to every sqlite connection
*/
int feisty_db_init(const char *dummy)
{
	int nErr = 0;

	nErr += sqlite3_auto_extension((void *)sqlite3_carray_init);
	nErr += sqlite3_auto_extension((void *)sqlite3_decimal_init);
	nErr += sqlite3_auto_extension((void *)sqlite3_ieee_init);
	nErr += sqlite3_auto_extension((void *)sqlite3_series_init);
	nErr += sqlite3_auto_extension((void *)sqlite3_shathree_init);
	nErr += sqlite3_auto_extension((void *)sqlite3_uuid_init);

	return nErr ? SQLITE_ERROR : SQLITE_OK;
}

/************************** End of FeistyDB additions *********************/
EOF
fi

/bin/cp "$SQLITE_DIR/sqlite3.c" "./Sources/CSQLite/"
/bin/cp "$SQLITE_DIR/sqlite3.h" "./Sources/CSQLite/include/"
/bin/cp "$SQLITE_DIR/sqlite3ext.h" "./Sources/CSQLite/include/"
/bin/cp "$SQLITE_DIR/ext/misc/carray.h" "./Sources/CSQLite/include/"

echo "sqlite successfully configured for FeistyDB"
