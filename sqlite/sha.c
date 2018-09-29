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
** This SQLite extension implements functions for creating crytographic
** hashes.
**
*/
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <assert.h>

#include <CommonCrypto/CommonDigest.h>

/*
** Implementation of sha1() function.
*/
static void sha1func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert(argc==1);
  switch(sqlite3_value_type(argv[0])) {
    case SQLITE_BLOB:
    {
      unsigned char md [CC_SHA1_DIGEST_LENGTH];
      CC_SHA1(sqlite3_value_blob(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA1_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_TEXT:
    {
      unsigned char md [CC_SHA1_DIGEST_LENGTH];
      CC_SHA1(sqlite3_value_text(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA1_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_NULL:
      sqlite3_result_null(context);
      break;
    default:
      sqlite3_result_error(context, "sha1 only supports BLOB, TEXT, and NULL types", -1);
      break;
  }
}

/*
** Implementation of sha224() function.
*/
static void sha224func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert(argc==1);
  switch(sqlite3_value_type(argv[0])) {
    case SQLITE_BLOB:
    {
      unsigned char md [CC_SHA224_DIGEST_LENGTH];
      CC_SHA224(sqlite3_value_blob(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA224_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_TEXT:
    {
      unsigned char md [CC_SHA224_DIGEST_LENGTH];
      CC_SHA224(sqlite3_value_text(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA224_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_NULL:
      sqlite3_result_null(context);
      break;
    default:
      sqlite3_result_error(context, "sha224 only supports BLOB, TEXT, and NULL types", -1);
      break;
  }
}

/*
** Implementation of sha256() function.
*/
static void sha256func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert(argc==1);
  switch(sqlite3_value_type(argv[0])) {
    case SQLITE_BLOB:
    {
      unsigned char md [CC_SHA256_DIGEST_LENGTH];
      CC_SHA256(sqlite3_value_blob(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA256_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_TEXT:
    {
      unsigned char md [CC_SHA256_DIGEST_LENGTH];
      CC_SHA256(sqlite3_value_text(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA256_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_NULL:
      sqlite3_result_null(context);
      break;
    default:
      sqlite3_result_error(context, "sha256 only supports BLOB, TEXT, and NULL types", -1);
      break;
  }
}

/*
** Implementation of sha384() function.
*/
static void sha384func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert(argc==1);
  switch(sqlite3_value_type(argv[0])) {
    case SQLITE_BLOB:
    {
      unsigned char md [CC_SHA384_DIGEST_LENGTH];
      CC_SHA384(sqlite3_value_blob(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA384_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_TEXT:
    {
      unsigned char md [CC_SHA384_DIGEST_LENGTH];
      CC_SHA384(sqlite3_value_text(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA384_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_NULL:
      sqlite3_result_null(context);
      break;
    default:
      sqlite3_result_error(context, "sha384 only supports BLOB, TEXT, and NULL types", -1);
      break;
  }
}

/*
** Implementation of sha512() function.
*/
static void sha512func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert(argc==1);
  switch(sqlite3_value_type(argv[0])) {
    case SQLITE_BLOB:
    {
      unsigned char md [CC_SHA512_DIGEST_LENGTH];
      CC_SHA512(sqlite3_value_blob(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA512_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_TEXT:
    {
      unsigned char md [CC_SHA512_DIGEST_LENGTH];
      CC_SHA512(sqlite3_value_text(argv[0]), (CC_LONG)sqlite3_value_bytes(argv[0]), md);
      sqlite3_result_blob(context, md, CC_SHA512_DIGEST_LENGTH, SQLITE_TRANSIENT);
      break;
    }
    case SQLITE_NULL:
      sqlite3_result_null(context);
      break;
    default:
      sqlite3_result_error(context, "sha512 only supports BLOB, TEXT, and NULL types", -1);
      break;
  }
}

/*
** Register SHA functions to database `db`.
*/
static int register_sha_functions(sqlite3 *db) {
  typedef struct SHAScalar {
    const char *name;
    int argc;
    int enc;
    void (*func)(sqlite3_context*, int, sqlite3_value**);
  } SHAScalar;

  SHAScalar scalars[] = {
    {"sha1",                1, SQLITE_UTF8, sha1func},
    {"sha224",              1, SQLITE_UTF8, sha224func},
    {"sha256",              1, SQLITE_UTF8, sha256func},
    {"sha384",              1, SQLITE_UTF8, sha384func},
    {"sha512",              1, SQLITE_UTF8, sha512func},
  };

  int rc = SQLITE_OK;
  int i, n;

  n = (int)(sizeof(scalars)/sizeof(scalars[0]));

  for (i = 0; rc == SQLITE_OK && i < n; i++) {
    SHAScalar *s = &scalars[i];
    rc = sqlite3_create_function(db, s->name, s->argc, s->enc, 0, s->func, 0, 0);
  }

  return rc;
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_sha_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  return register_sha_functions(db);
}
