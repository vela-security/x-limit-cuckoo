/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @brief Lua cuckoo_filter implementation @file */

#include <limits.h>
#include <math.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <lauxlib.h>
#include <lua.h>

#include "common.h"
#include "xxhash.h"
#include "snappy.h"


static const char *module_name  = "vela.cuckoo";
static const char *module_table = "cuckoo";


typedef struct cuckoo_bucket
{
  uint16_t entries[BUCKET_SIZE];
} cuckoo_bucket;

typedef struct cuckoo_header
{
  size_t nbytes;
  size_t items;
  size_t bytes;
  size_t num_buckets;
  size_t cnt;
  size_t exdata;
  size_t total;
  int nlz;
} cuckoo_header;


typedef struct cuckoo_filter
{
  size_t nbytes;
  size_t items;
  size_t bytes;
  size_t num_buckets;
  size_t cnt;
  size_t exdata;
  size_t total;
  int nlz;
  cuckoo_bucket buckets[];
} cuckoo_filter;

static int lsnappy_compress(lua_State *L)
{
    size_t src_len = 0;
	struct snappy_env env;
	snappy_init_env(&env);

    const char* src = luaL_checklstring(L, 1, &src_len);
    size_t dst_max_size = snappy_max_compressed_length(src_len);
    char* dstmem = (char*)malloc(dst_max_size);
    if (dstmem != NULL) {
        if (snappy_compress(&env , src, src_len, dstmem, &dst_max_size) == SNAPPY_OK) {
            lua_pushlstring(L, dstmem, dst_max_size);
            free(dstmem);
            snappy_free_env(&env);
            return 1;
        }
        free(dstmem);
        snappy_free_env(&env);
    }
    snappy_free_env(&env);
    return luaL_error(L, "snappy: not enough memory.");
}

static int lsnappy_uncompress(lua_State *L)
{
    size_t src_len = 0;
    size_t dst_max_size = 0;
    const char* src = luaL_checklstring(L, 1, &src_len);
    if (!snappy_uncompressed_length(src, src_len, &dst_max_size)) {
        lua_pushlstring(L, "", 0);
    }
    char* dstmem = (char*)malloc(dst_max_size);
    if (dstmem != NULL) {
        if (snappy_uncompress(src, src_len, dstmem) == SNAPPY_OK) {
            lua_pushlstring(L, dstmem, dst_max_size);
            free(dstmem);
            return 1;
        }
        free(dstmem);
    }
    return luaL_error(L, "snappy: not enough memory.");
}

static int lsnappy_validate_compressed_buffer(lua_State *L) 
{
    size_t src_len = 0;
    size_t dst_max_size = 0;
    const char* src = luaL_checklstring(L, 1, &src_len);
    if(snappy_validate_compressed_buffer(src, src_len) == SNAPPY_OK) {
        lua_pushboolean(L, 1);
        return 1;
    }
    lua_pushboolean(L, 0);
    return 1;
}

static void
verify_item(lua_State* lua, const void **item, size_t *len, lua_Number *d)
{
  switch (lua_type(lua, 1)) {
  case LUA_TSTRING:
    *item = lua_tolstring(lua, 1, len);
    break;
  case LUA_TNUMBER:
    {
      *d = lua_tonumber(lua, 1);
      *item = d;
      *len = sizeof(lua_Number);
    }
    break;
  default:
    luaL_typerror(lua, 1, "string or number");
    break;
  }
}

static int h32(lua_State* lua)
{
  const void *item = NULL;
  size_t len = 0;
  lua_Number d;
  verify_item(lua, &item, &len, &d);
  lua_Number n = luaL_optnumber(lua, 2, 0);
  luaL_argcheck(lua, n >=0 && n <= UINT_MAX, 2, "seed must be an unsigned int");
  unsigned seed = (unsigned)n;
  lua_pushnumber(lua, XXH32(item, len, seed));
  return 1;
}


static int h64(lua_State* lua)
{
  const void *item = NULL;
  size_t len = 0;
  lua_Number d;
  verify_item(lua, &item, &len, &d);
  lua_Number n = luaL_optnumber(lua, 2, 0);
  luaL_argcheck(lua, n >=0 && n <= ULLONG_MAX, 2,
                "seed must be an unsigned long long");
  unsigned long long seed = (unsigned long long)n;
  lua_pushnumber(lua, XXH64(item, len, seed));
  return 1;
}


static int cuckoo_new(lua_State *lua)
{
  int n = lua_gettop(lua);
  luaL_argcheck(lua, n == 1, 0, "incorrect number of arguments");
  int items = luaL_checkint(lua, 1);
  luaL_argcheck(lua, items > 4, 1, "items must be > 4");

  if (items < 32) {
      items = 32;
  }

  unsigned buckets  = clp2((unsigned)ceil(items / BUCKET_SIZE));
  size_t bytes      = sizeof(cuckoo_bucket) * buckets;
  size_t nbytes     = sizeof(cuckoo_filter) + bytes;
  cuckoo_filter *cf = (cuckoo_filter *)lua_newuserdata(lua, nbytes);
  cf->items         = buckets * BUCKET_SIZE;
  cf->num_buckets   = buckets;
  cf->bytes         = bytes;
  cf->cnt           = 0;
  cf->total         = 0;
  cf->exdata        = 0;
  cf->nlz           = nlz(buckets) + 1;
  cf->nbytes        = nbytes;
  memset(cf->buckets, 0, cf->bytes);
  luaL_getmetatable(lua, module_name);
  lua_setmetatable(lua, -2);
  return 1;
}


static cuckoo_filter* check_cuckoo_ud(lua_State *lua, int args)
{
  cuckoo_filter *cf = luaL_checkudata(lua, 1, module_name);
  luaL_argcheck(lua, args == lua_gettop(lua), 0,
                "incorrect number of arguments");
  return cf;
}


static bool bucket_lookup(cuckoo_bucket *b, uint16_t fp)
{
  int i;
  for (i = 0; i < BUCKET_SIZE; ++i) {
    if (b->entries[i] == fp) return true;
  }
  return false;
}


static bool bucket_delete(cuckoo_bucket *b, uint16_t fp)
{
  int i;
  for (i = 0; i < BUCKET_SIZE; ++i) {
    if (b->entries[i] == fp) {
      b->entries[i] = 0;
      return true;
    }
  }
  return false;
}


static bool bucket_add(cuckoo_bucket *b, uint16_t fp)
{
  int i;
  for (i = 0; i < BUCKET_SIZE; ++i) {
    if (b->entries[i] == 0) {
      b->entries[i] = fp;
      return true;
    }
  }
  return false;
}


static int8_t bucket_insert(lua_State *lua, cuckoo_filter *cf, unsigned i1,
                          unsigned i2, uint16_t fp)
{
  // since we must handle duplicates we consider any collision within the bucket
  // to be a duplicate. The 16 bit fingerprint makes the false postive rate very
  // low 0.00012
  if (bucket_lookup(&cf->buckets[i1], fp)) return 0;
  if (bucket_lookup(&cf->buckets[i2], fp)) return 0;

  if (!bucket_add(&cf->buckets[i1], fp)) {
    if (!bucket_add(&cf->buckets[i2], fp)) {
      unsigned ri;
      if (rand() % 2) {
        ri = i1;
      } else {
        ri = i2;
      }
      int i;
      for (i = 0; i < 512; ++i) {
        int entry = rand() % BUCKET_SIZE;
        unsigned tmp = cf->buckets[ri].entries[entry];
        cf->buckets[ri].entries[entry] = fp;
        fp = tmp;
        ri = ri ^ (XXH64(&fp, sizeof(uint16_t), 1) >> (cf->nlz + 32));
        if (bucket_lookup(&cf->buckets[ri], fp)) return 0;
        if (bucket_add(&cf->buckets[ri], fp)) return 1;
      }
      return 2;
    }
  }
  return 1;
}


static int cuckoo_add(lua_State *lua)
{
  cuckoo_filter *ud = check_cuckoo_ud(lua, 2);
  size_t len = 0;
  double val = 0;
  void *key = NULL;

  switch (lua_type(lua, 2)) {
  case LUA_TSTRING:
    key = (void *)lua_tolstring(lua, 2, &len);
    break;
  case LUA_TNUMBER:
    val = lua_tonumber(lua, 2);
    len = sizeof(double);
    key = &val;
    break;
  default:
    luaL_argerror(lua, 2, "must be a string or number");
    break;
  }
  ++ud->total;

  uint64_t h = XXH64(key, (int)len, 1);
  uint16_t fp = fingerprint16(h);
  unsigned i1 = h % ud->num_buckets;
  unsigned i2 = i1 ^ (XXH64(&fp, sizeof(uint16_t), 1) >> (ud->nlz + 32));
  uint8_t ret = bucket_insert(lua, ud, i1, i2, fp);
  switch (ret) {
  case 0:
        lua_pushnumber(lua , ret);
        lua_pushnumber(lua, ud->cnt);
        return 2;

  case 1:
        ++ud->cnt;
        lua_pushnumber(lua, ret);
        lua_pushnumber(lua, ud->cnt);
        return 2;

  case 2:
        lua_pushnumber(lua, ret);
        lua_pushnumber(lua, ud->cnt);
        return 2;
  }
}


static int cuckoo_query(lua_State *lua)
{
  cuckoo_filter *cf = check_cuckoo_ud(lua, 2);
  size_t len = 0;
  double val = 0;
  void *key = NULL;
  switch (lua_type(lua, 2)) {
  case LUA_TSTRING:
    key = (void *)lua_tolstring(lua, 2, &len);
    break;
  case LUA_TNUMBER:
    val = lua_tonumber(lua, 2);
    len = sizeof(double);
    key = &val;
    break;
  default:
    luaL_argerror(lua, 2, "must be a string or number");
    break;
  }
  uint64_t h = XXH64(key, (int)len, 1);
  uint16_t fp = fingerprint16(h);
  unsigned i1 = h % cf->num_buckets;
  bool found = bucket_lookup(&cf->buckets[i1], fp);
  if (!found) {
    unsigned i2 = i1 ^ (XXH64(&fp, sizeof(uint16_t), 1) >> (cf->nlz + 32));
    found = bucket_lookup(&cf->buckets[i2], fp);
  }
  lua_pushboolean(lua, found);
  return 1;
}


static int cuckoo_delete(lua_State *lua)
{
  cuckoo_filter *cf = check_cuckoo_ud(lua, 2);
  size_t len = 0;
  double val = 0;
  void *key = NULL;
  switch (lua_type(lua, 2)) {
  case LUA_TSTRING:
    key = (void *)lua_tolstring(lua, 2, &len);
    break;
  case LUA_TNUMBER:
    val = lua_tonumber(lua, 2);
    len = sizeof(double);
    key = &val;
    break;
  default:
    luaL_argerror(lua, 2, "must be a string or number");
    break;
  }
  uint64_t h = XXH64(key, (int)len, 1);
  uint16_t fp = fingerprint16(h);
  unsigned i1 = h % cf->num_buckets;
  bool deleted = bucket_delete(&cf->buckets[i1], fp);
  if (!deleted) {
    unsigned i2 = i1 ^ (XXH64(&fp, sizeof(uint16_t), 1) >> (cf->nlz + 32));
    deleted = bucket_delete(&cf->buckets[i2], fp);
  }
  if (deleted) {
    --cf->cnt;
  }
  lua_pushboolean(lua, deleted);
  return 1;
}


static int cuckoo_count(lua_State *lua)
{
  cuckoo_filter *cf = check_cuckoo_ud(lua, 1);
  lua_pushnumber(lua, (lua_Number)cf->cnt);
  return 1;
}


static int cuckoo_clear(lua_State *lua)
{
  cuckoo_filter *cf = check_cuckoo_ud(lua, 1);
  memset(cf->buckets, 0, cf->bytes);
  cf->cnt = 0;
  return 0;
}

static int cuckoo_encode(lua_State *lua)
{
  cuckoo_filter *ud = check_cuckoo_ud(lua, 2);
  bool compress = lua_toboolean(lua , 2);
  if (compress) {
      struct snappy_env env;
      snappy_init_env(&env);
      size_t dst_max_size = snappy_max_compressed_length(ud->nbytes);
      char* dstmem = (char*)malloc(dst_max_size);
      if (dstmem == NULL) {
        return error_x(lua, "snappy: not enough memory.");
      }

      if (snappy_compress(&env , (char *)ud, ud->nbytes , dstmem, &dst_max_size) == SNAPPY_OK) {
        lua_pushlstring(lua, dstmem, dst_max_size);
        free(dstmem);
        snappy_free_env(&env);
        return 1;
      }

      free(dstmem);
      snappy_free_env(&env);
      return error_x(lua , "snapp: compress fail");
  }

  lua_pushlstring(lua , (char *) ud , ud->nbytes);
  return 1;
}


static int cuckoo_decode(lua_State *lua)
{
  size_t src_len = 0;
  char  *data;
  const char* src = luaL_checklstring(lua, 1, &src_len);

  bool  compress = lua_toboolean(lua , 2);
  if (src_len <= 0) { return error_x(lua , "empty"); }

  if (compress) {
      //uncompress
      size_t max_size = 0;
      if (!snappy_uncompressed_length(src, src_len, &max_size)) {
          return error_x(lua , "snapp: uncompress length");
      }

      if ( max_size < sizeof(cuckoo_filter))  { //size not match
          return error_x(lua , "too small");                                    
      }

      cuckoo_filter *u2 = (cuckoo_filter *)lua_newuserdata(lua, max_size);
      if (u2 == NULL) {
          return error_x(lua , "init fail");
      }

      if (snappy_uncompress(src, src_len, (char *)u2) != SNAPPY_OK)  {
          return error_x(lua , "snappy uncompress fail");
      }

      if (u2->nbytes != max_size) {
          return error_x(lua , "size not match");
      } 

      luaL_getmetatable(lua, module_name);
      lua_setmetatable(lua, -2);
      return 1;
  }


  if (src_len < sizeof(cuckoo_filter)) {
      return error_x(lua , "too small");                                    
  } 

  cuckoo_header *h2 = (cuckoo_header *)src;
  if (h2->nbytes != src_len) {
      return error_x(lua , "size not match");
  }

  cuckoo_filter *u2 = (cuckoo_filter *)lua_newuserdata(lua, src_len);
  memcpy(u2 , src, src_len);
  luaL_getmetatable(lua, module_name);
  lua_setmetatable(lua, -2);
  return 1;
}

static int cuckoo_cast(lua_State *lua)
{
  size_t size;
  size = luaL_checkint(lua , 2);
  if (size < sizeof(cuckoo_filter)) {
      return 0;
  }

  void *u1 = (void *)luaL_checkstring(lua , 1);
  cuckoo_filter *u2 = (cuckoo_filter *)lua_newuserdata(lua, size);
  memcpy(u2 , u1 , size);
  luaL_getmetatable(lua, module_name);
  lua_setmetatable(lua, -2);
  return 1;
}


static int cuckoo_bytes(lua_State *lua)
{
  cuckoo_filter *cf = check_cuckoo_ud(lua, 1);
  lua_pushnumber(lua, cf->bytes);
  return 1;
}

static int cuckoo_exdata(lua_State *lua)
{
  size_t n = lua_gettop(lua);
  if (n == 1 ) {
    cuckoo_filter *ud = check_cuckoo_ud(lua, 1);
    lua_pushnumber(lua , (lua_Number)ud->exdata);
    return 1;
  }

  cuckoo_filter *ud = check_cuckoo_ud(lua, 2);
  size_t exdata = luaL_checkint(lua , 2);
  ud->exdata = exdata; 
  return 0;
}

static int cuckoo_total(lua_State *lua)
{
  size_t n = lua_gettop(lua);
  if (n == 1 ) {
    cuckoo_filter *ud = check_cuckoo_ud(lua, 1);
    lua_pushnumber(lua , (lua_Number)ud->total);
    return 1;
  }

  cuckoo_filter *ud = check_cuckoo_ud(lua, 2);
  size_t x = luaL_checkint(lua , 2);
  ud->total = ud->total + x; 
  return 0;
}

static struct luaL_Reg cuckoo_lib_f[] =
{
  { "new", cuckoo_new },
  { "decode", cuckoo_decode },
  { "cast", cuckoo_cast },
  { "h32", h32 },
  { "h64", h64 },
  { "compress", lsnappy_compress},
  { "uncompress", lsnappy_uncompress},
  { NULL, NULL }
};

static struct luaL_Reg cuckoo_lib_m[] =
{
  { "add"    , cuckoo_add },
  { "put"    , cuckoo_add },
  { "exdata" , cuckoo_exdata },
  { "total" , cuckoo_total },
  { "query"  , cuckoo_query },
  { "delete" , cuckoo_delete },
  { "count"  , cuckoo_count },
  { "encode" , cuckoo_encode},
  { "bytes"  , cuckoo_bytes },
  { "clear"  , cuckoo_clear },
  { NULL     , NULL }
};


int luaopen_cuckoo(lua_State *lua)
{
  luaL_newmetatable(lua, module_name);
  lua_pushvalue(lua, -1);
  lua_setfield(lua, -2, "__index");
  luaL_register(lua, NULL, cuckoo_lib_m);
    
  lua_newtable(lua);
  luaL_setfuncs(lua, cuckoo_lib_f, 0);
  return 1;
}
