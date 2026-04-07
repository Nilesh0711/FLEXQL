#ifndef FLEXQL_INDEX_H
#define FLEXQL_INDEX_H

#include <stdint.h>
#include <stddef.h>

int index_file_create(const char* path);
int index_file_reset(const char* path);
int index_file_insert(const char* path, const char* key, uint64_t row_offset);
int index_file_insert_batch(const char* path, const char** keys, const uint64_t* row_offsets,
                            size_t count);
int index_file_find(const char* path, const char* key, uint64_t* row_offset, int* found);

#endif
