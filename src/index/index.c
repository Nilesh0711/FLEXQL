#include "index/index.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLEXQL_INDEX_MAGIC 0x465842505431ULL
#define FLEXQL_INDEX_KEY_SIZE 64
#define FLEXQL_BPT_MAX_KEYS 31
#define FLEXQL_INDEX_CACHE_PAGES 32768

typedef struct {
  unsigned long long magic;
  unsigned long long root_page;
  unsigned long long next_page;
} index_header;

typedef struct {
  unsigned int is_leaf;
  unsigned int key_count;
  unsigned long long next_leaf;
  char keys[FLEXQL_BPT_MAX_KEYS][FLEXQL_INDEX_KEY_SIZE];
  unsigned long long values[FLEXQL_BPT_MAX_KEYS + 1];
} index_page;

typedef struct {
  int has_split;
  char split_key[FLEXQL_INDEX_KEY_SIZE];
  unsigned long long right_page;
} split_result;

typedef struct {
  int used;
  int dirty;
  unsigned long long page_no;
  unsigned long long tick;
  index_page page;
} cache_entry;

typedef struct {
  FILE* fp;
  index_header hdr;
  cache_entry cache[FLEXQL_INDEX_CACHE_PAGES];
  unsigned long long tick;
} index_session;

static int read_header(FILE* fp, index_header* hdr) {
  if (fseek(fp, 0, SEEK_SET) != 0) {
    return 0;
  }
  if (fread(hdr, sizeof(*hdr), 1, fp) != 1) {
    return 0;
  }
  return hdr->magic == FLEXQL_INDEX_MAGIC;
}

static int write_header(FILE* fp, const index_header* hdr) {
  if (fseek(fp, 0, SEEK_SET) != 0) {
    return 0;
  }
  return fwrite(hdr, sizeof(*hdr), 1, fp) == 1;
}

static long page_offset(unsigned long long page_no) {
  return (long)(sizeof(index_header) + page_no * sizeof(index_page));
}

static int read_page_disk(FILE* fp, unsigned long long page_no, index_page* page) {
  if (fseek(fp, page_offset(page_no), SEEK_SET) != 0) {
    return 0;
  }
  return fread(page, sizeof(*page), 1, fp) == 1;
}

static int write_page_disk(FILE* fp, unsigned long long page_no, const index_page* page) {
  if (fseek(fp, page_offset(page_no), SEEK_SET) != 0) {
    return 0;
  }
  return fwrite(page, sizeof(*page), 1, fp) == 1;
}

static void key_copy(char dst[FLEXQL_INDEX_KEY_SIZE], const char* src) {
  size_t n = strlen(src);
  if (n >= FLEXQL_INDEX_KEY_SIZE) {
    n = FLEXQL_INDEX_KEY_SIZE - 1;
  }
  memcpy(dst, src, n);
  dst[n] = '\0';
}

static int key_cmp(const char* a, const char* b) { return strcmp(a, b); }

static cache_entry* cache_find(index_session* s, unsigned long long page_no) {
  size_t idx = page_no % FLEXQL_INDEX_CACHE_PAGES;
  if (s->cache[idx].used && s->cache[idx].page_no == page_no) {
    s->tick += 1;
    s->cache[idx].tick = s->tick;
    return &s->cache[idx];
  }
  return NULL;
}

static int cache_flush_entry(index_session* s, cache_entry* entry) {
  if (!entry->used || !entry->dirty) {
    return 1;
  }
  if (!write_page_disk(s->fp, entry->page_no, &entry->page)) {
    return 0;
  }
  entry->dirty = 0;
  return 1;
}

static cache_entry* cache_acquire(index_session* s, unsigned long long page_no, int create) {
  size_t idx = page_no % FLEXQL_INDEX_CACHE_PAGES;
  cache_entry* victim = &s->cache[idx];

  if (victim->used && victim->page_no == page_no) {
    s->tick += 1;
    victim->tick = s->tick;
    return victim;
  }

  if (!cache_flush_entry(s, victim)) {
    return NULL;
  }

  memset(victim, 0, sizeof(*victim));
  victim->used = 1;
  victim->page_no = page_no;
  s->tick += 1;
  victim->tick = s->tick;

  if (create) {
    memset(&victim->page, 0, sizeof(victim->page));
    return victim;
  }
  if (!read_page_disk(s->fp, page_no, &victim->page)) {
    memset(victim, 0, sizeof(*victim));
    return NULL;
  }
  return victim;
}

static int session_flush(index_session* s) {
  size_t i;
  for (i = 0; i < FLEXQL_INDEX_CACHE_PAGES; ++i) {
    if (!cache_flush_entry(s, &s->cache[i])) {
      return 0;
    }
  }
  if (!write_header(s->fp, &s->hdr)) {
    return 0;
  }
  return fflush(s->fp) == 0;
}

static int session_open(index_session* s, const char* path) {
  s->fp = fopen(path, "r+b");
  if (s->fp == NULL) {
    s->fp = fopen(path, "w+b");
  }
  if (s->fp == NULL) {
    return 0;
  }
  if (!read_header(s->fp, &s->hdr)) {
    index_page root;
    s->hdr.magic = FLEXQL_INDEX_MAGIC;
    s->hdr.root_page = 0;
    s->hdr.next_page = 1;
    memset(&root, 0, sizeof(root));
    root.is_leaf = 1;
    if (!write_header(s->fp, &s->hdr) || !write_page_disk(s->fp, 0, &root) || fflush(s->fp) != 0) {
      fclose(s->fp);
      s->fp = NULL;
      return 0;
    }
  }
  return 1;
}

static void session_close(index_session* s) {
  if (s->fp != NULL) {
    fclose(s->fp);
    s->fp = NULL;
  }
}

static int alloc_page(index_session* s, unsigned long long* page_no) {
  cache_entry* entry;
  *page_no = s->hdr.next_page++;
  entry = cache_acquire(s, *page_no, 1);
  if (entry == NULL) {
    return 0;
  }
  entry->dirty = 1;
  return 1;
}

static void page_insert_key(index_page* page, int pos, const char* key, unsigned long long value,
                            unsigned long long child_after) {
  int i;
  for (i = (int)page->key_count; i > pos; --i) {
    memcpy(page->keys[i], page->keys[i - 1], FLEXQL_INDEX_KEY_SIZE);
    if (page->is_leaf) {
      page->values[i] = page->values[i - 1];
    } else {
      page->values[i + 1] = page->values[i];
    }
  }
  key_copy(page->keys[pos], key);
  if (page->is_leaf) {
    page->values[pos] = value;
  } else {
    page->values[pos + 1] = child_after;
  }
  page->key_count += 1;
}

static int insert_recursive(index_session* s, unsigned long long page_no, const char* key,
                            unsigned long long row_offset, split_result* result) {
  cache_entry* entry = cache_acquire(s, page_no, 0);
  index_page* page;
  int pos = 0;
  if (entry == NULL) {
    return 0;
  }
  page = &entry->page;

  while (pos < (int)page->key_count && key_cmp(page->keys[pos], key) < 0) {
    pos++;
  }

  if (page->is_leaf) {
    if (pos < (int)page->key_count && key_cmp(page->keys[pos], key) == 0) {
      result->has_split = 0;
      return 1;
    }

    if (page->key_count < FLEXQL_BPT_MAX_KEYS) {
      page_insert_key(page, pos, key, row_offset, 0);
      entry->dirty = 1;
      result->has_split = 0;
      return 1;
    }

    {
      char temp_keys[FLEXQL_BPT_MAX_KEYS + 1][FLEXQL_INDEX_KEY_SIZE];
      unsigned long long temp_vals[FLEXQL_BPT_MAX_KEYS + 1];
      cache_entry* right_entry;
      unsigned long long right_page_no;
      int i;
      int split_at;

      for (i = 0; i < pos; ++i) {
        memcpy(temp_keys[i], page->keys[i], FLEXQL_INDEX_KEY_SIZE);
        temp_vals[i] = page->values[i];
      }
      key_copy(temp_keys[pos], key);
      temp_vals[pos] = row_offset;
      for (i = pos; i < (int)page->key_count; ++i) {
        memcpy(temp_keys[i + 1], page->keys[i], FLEXQL_INDEX_KEY_SIZE);
        temp_vals[i + 1] = page->values[i];
      }

      split_at = (FLEXQL_BPT_MAX_KEYS + 1) / 2;
      if (!alloc_page(s, &right_page_no)) {
        return 0;
      }
      right_entry = cache_acquire(s, right_page_no, 1);
      if (right_entry == NULL) {
        return 0;
      }
      memset(&right_entry->page, 0, sizeof(right_entry->page));
      right_entry->page.is_leaf = 1;
      right_entry->page.next_leaf = page->next_leaf;
      right_entry->page.key_count = (unsigned int)((FLEXQL_BPT_MAX_KEYS + 1) - split_at);

      page->next_leaf = right_page_no;
      page->key_count = (unsigned int)split_at;
      for (i = 0; i < split_at; ++i) {
        memcpy(page->keys[i], temp_keys[i], FLEXQL_INDEX_KEY_SIZE);
        page->values[i] = temp_vals[i];
      }
      for (i = split_at; i < FLEXQL_BPT_MAX_KEYS + 1; ++i) {
        if (i - split_at < (int)right_entry->page.key_count) {
          memcpy(right_entry->page.keys[i - split_at], temp_keys[i], FLEXQL_INDEX_KEY_SIZE);
          right_entry->page.values[i - split_at] = temp_vals[i];
        }
      }
      entry->dirty = 1;
      right_entry->dirty = 1;

      result->has_split = 1;
      key_copy(result->split_key, right_entry->page.keys[0]);
      result->right_page = right_page_no;
      return 1;
    }
  }

  {
    split_result child = {0};
    unsigned long long child_page = page->values[pos];
    if (!insert_recursive(s, child_page, key, row_offset, &child)) {
      return 0;
    }
    if (!child.has_split) {
      result->has_split = 0;
      return 1;
    }

    if (page->key_count < FLEXQL_BPT_MAX_KEYS) {
      page_insert_key(page, pos, child.split_key, 0, child.right_page);
      entry->dirty = 1;
      result->has_split = 0;
      return 1;
    }

    {
      char temp_keys[FLEXQL_BPT_MAX_KEYS + 1][FLEXQL_INDEX_KEY_SIZE];
      unsigned long long temp_children[FLEXQL_BPT_MAX_KEYS + 2];
      cache_entry* right_entry;
      unsigned long long right_page_no;
      int i;
      int split_at;

      for (i = 0; i <= (int)page->key_count; ++i) {
        temp_children[i] = page->values[i];
      }
      for (i = 0; i < pos; ++i) {
        memcpy(temp_keys[i], page->keys[i], FLEXQL_INDEX_KEY_SIZE);
      }
      key_copy(temp_keys[pos], child.split_key);
      temp_children[pos + 1] = child.right_page;
      for (i = pos; i < (int)page->key_count; ++i) {
        memcpy(temp_keys[i + 1], page->keys[i], FLEXQL_INDEX_KEY_SIZE);
        temp_children[i + 2] = page->values[i + 1];
      }

      split_at = (FLEXQL_BPT_MAX_KEYS + 1) / 2;
      if (!alloc_page(s, &right_page_no)) {
        return 0;
      }
      right_entry = cache_acquire(s, right_page_no, 1);
      if (right_entry == NULL) {
        return 0;
      }
      memset(&right_entry->page, 0, sizeof(right_entry->page));
      right_entry->page.is_leaf = 0;
      right_entry->page.key_count = (unsigned int)(FLEXQL_BPT_MAX_KEYS - split_at);

      page->key_count = (unsigned int)split_at;
      for (i = 0; i < split_at; ++i) {
        memcpy(page->keys[i], temp_keys[i], FLEXQL_INDEX_KEY_SIZE);
        page->values[i] = temp_children[i];
      }
      page->values[split_at] = temp_children[split_at];

      for (i = 0; i < (int)right_entry->page.key_count; ++i) {
        memcpy(right_entry->page.keys[i], temp_keys[split_at + 1 + i], FLEXQL_INDEX_KEY_SIZE);
        right_entry->page.values[i] = temp_children[split_at + 1 + i];
      }
      right_entry->page.values[right_entry->page.key_count] = temp_children[FLEXQL_BPT_MAX_KEYS + 1];
      entry->dirty = 1;
      right_entry->dirty = 1;

      result->has_split = 1;
      key_copy(result->split_key, temp_keys[split_at]);
      result->right_page = right_page_no;
      return 1;
    }
  }
}

static int index_insert_session(index_session* s, const char* key, uint64_t row_offset) {
  split_result result = {0};
  if (!insert_recursive(s, s->hdr.root_page, key, row_offset, &result)) {
    return 0;
  }
  if (result.has_split) {
    cache_entry* root_entry;
    unsigned long long new_root_no;
    if (!alloc_page(s, &new_root_no)) {
      return 0;
    }
    root_entry = cache_acquire(s, new_root_no, 1);
    if (root_entry == NULL) {
      return 0;
    }
    memset(&root_entry->page, 0, sizeof(root_entry->page));
    root_entry->page.is_leaf = 0;
    root_entry->page.key_count = 1;
    key_copy(root_entry->page.keys[0], result.split_key);
    root_entry->page.values[0] = s->hdr.root_page;
    root_entry->page.values[1] = result.right_page;
    root_entry->dirty = 1;
    s->hdr.root_page = new_root_no;
  }
  return 1;
}

int index_file_create(const char* path) {
  index_session* s = calloc(1, sizeof(index_session));
  if (s == NULL) return 0;
  if (!session_open(s, path)) {
    free(s);
    return 0;
  }
  if (!session_flush(s)) {
    session_close(s);
    free(s);
    return 0;
  }
  session_close(s);
  free(s);
  return 1;
}

int index_file_reset(const char* path) {
  FILE* fp = fopen(path, "w+b");
  index_header hdr;
  index_page root;
  if (fp == NULL) {
    return 0;
  }
  hdr.magic = FLEXQL_INDEX_MAGIC;
  hdr.root_page = 0;
  hdr.next_page = 1;
  memset(&root, 0, sizeof(root));
  root.is_leaf = 1;
  if (!write_header(fp, &hdr) || !write_page_disk(fp, 0, &root) || fflush(fp) != 0) {
    fclose(fp);
    return 0;
  }
  fclose(fp);
  return 1;
}

int index_file_insert(const char* path, const char* key, uint64_t row_offset) {
  index_session* s = calloc(1, sizeof(index_session));
  if (s == NULL) return 0;
  if (!session_open(s, path)) {
    free(s);
    return 0;
  }
  if (!index_insert_session(s, key, row_offset) || !session_flush(s)) {
    session_close(s);
    free(s);
    return 0;
  }
  session_close(s);
  free(s);
  return 1;
}

int index_file_insert_batch(const char* path, const char** keys, const uint64_t* row_offsets,
                            size_t count) {
  index_session* s = calloc(1, sizeof(index_session));
  size_t i;
  if (count == 0) {
    if (s != NULL) free(s);
    return 1;
  }
  if (s == NULL) return 0;
  if (!session_open(s, path)) {
    free(s);
    return 0;
  }
  for (i = 0; i < count; ++i) {
    if (!index_insert_session(s, keys[i], row_offsets[i])) {
      session_close(s);
      free(s);
      return 0;
    }
  }
  if (!session_flush(s)) {
    session_close(s);
    free(s);
    return 0;
  }
  session_close(s);
  free(s);
  return 1;
}

int index_file_find(const char* path, const char* key, uint64_t* row_offset, int* found) {
  index_session* s = calloc(1, sizeof(index_session));
  unsigned long long page_no;
  if (found != NULL) {
    *found = 0;
  }
  if (s == NULL) return 0;
  if (!session_open(s, path)) {
    free(s);
    return 0;
  }
  page_no = s->hdr.root_page;
  for (;;) {
    cache_entry* entry = cache_acquire(s, page_no, 0);
    index_page* page;
    int pos = 0;
    if (entry == NULL) {
      session_close(s);
      free(s);
      return 0;
    }
    page = &entry->page;
    while (pos < (int)page->key_count && key_cmp(page->keys[pos], key) < 0) {
      pos++;
    }
    if (page->is_leaf) {
      if (pos < (int)page->key_count && key_cmp(page->keys[pos], key) == 0) {
        if (row_offset != NULL) {
          *row_offset = page->values[pos];
        }
        if (found != NULL) {
          *found = 1;
        }
      }
      session_close(s);
      free(s);
      return 1;
    }
    page_no = page->values[pos];
  }
}

