/*
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This SQLite extension implements functions for creating xxh
** hashes.
**
*/
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <assert.h>

#include "xxhash.h"

/*
** Implementation of xxh32() function.
*/
static void xxh32func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert(argc==1);
  switch(sqlite3_value_type(argv[0])) {
    case SQLITE_BLOB:
    {
      unsigned int hash = XXH32(sqlite3_value_blob(argv[0]), sqlite3_value_bytes(argv[0]), 0);
      sqlite3_result_int(context, hash);
      break;
    }
    case SQLITE_TEXT:
    {
      unsigned int hash = XXH32(sqlite3_value_text(argv[0]), sqlite3_value_bytes(argv[0]), 0);
      sqlite3_result_int(context, hash);
      break;
    }
    case SQLITE_NULL:
      sqlite3_result_null(context);
      break;
    default:
      sqlite3_result_error(context, "xxh32 only supports BLOB, TEXT, and NULL types", -1);
      break;
  }
}

/*
** Implementation of xxh64() function.
*/
static void xxh64func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert(argc==1);
  switch(sqlite3_value_type(argv[0])) {
    case SQLITE_BLOB:
    {
      unsigned long long hash = XXH64(sqlite3_value_blob(argv[0]), sqlite3_value_bytes(argv[0]), 0);
      sqlite3_result_int64(context, hash);
      break;
    }
    case SQLITE_TEXT:
    {
      unsigned long long hash = XXH64(sqlite3_value_text(argv[0]), sqlite3_value_bytes(argv[0]), 0);
      sqlite3_result_int64(context, hash);
      break;
    }
    case SQLITE_NULL:
      sqlite3_result_null(context);
      break;
    default:
      sqlite3_result_error(context, "xxh64 only supports BLOB, TEXT, and NULL types", -1);
      break;
  }
}

/*
** Register XXH functions to database `db`.
*/
static int register_xxh_functions(sqlite3 *db) {
  typedef struct XXHScalar {
    const char *name;
    int argc;
    int enc;
    void (*func)(sqlite3_context*, int, sqlite3_value**);
  } XXHScalar;

  XXHScalar scalars[] = {
    {"xxh32",               1, SQLITE_UTF8, xxh32func},
    {"xxh64",               1, SQLITE_UTF8, xxh64func},
  };

  int rc = SQLITE_OK;
  int i, n;

  n = (int)(sizeof(scalars)/sizeof(scalars[0]));

  for (i = 0; rc == SQLITE_OK && i < n; i++) {
    XXHScalar *s = &scalars[i];
    rc = sqlite3_create_function(db, s->name, s->argc, s->enc, 0, s->func, 0, 0);
  }

  return rc;
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_xxh_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  return register_xxh_functions(db);
}
