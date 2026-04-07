#include "parser/parser.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "utils/str_utils.h"

typedef struct {
  char* text;
  int quoted;
} token;

static void set_err(char** err, const char* msg) {
  if (err != NULL) {
    free(*err);
    *err = fx_strdup(msg);
  }
}

static int is_cmp_op(const char* s) {
  return strcmp(s, "=") == 0 || strcmp(s, ">") == 0 || strcmp(s, "<") == 0 ||
         strcmp(s, ">=") == 0 || strcmp(s, "<=") == 0 || strcmp(s, "!=") == 0;
}

static int token_push(token** toks, size_t* count, size_t* cap, const char* s, size_t n,
                      int quoted) {
  token t;
  if (*count == *cap) {
    size_t ncap = *cap == 0 ? 32 : *cap * 2;
    token* nt = (token*)realloc(*toks, ncap * sizeof(token));
    if (nt == NULL) {
      return 0;
    }
    *toks = nt;
    *cap = ncap;
  }
  t.text = fx_strndup(s, n);
  if (t.text == NULL) {
    return 0;
  }
  t.quoted = quoted;
  (*toks)[*count] = t;
  *count += 1;
  return 1;
}

static void tokens_free(token* toks, size_t count) {
  size_t i;
  for (i = 0; i < count; ++i) {
    free(toks[i].text);
  }
  free(toks);
}

static int tokenize(const char* sql, token** out_toks, size_t* out_count, char** err) {
  const char* p = sql;
  token* toks = NULL;
  size_t count = 0;
  size_t cap = 0;

  while (*p != '\0') {
    const char* start;
    if (isspace((unsigned char)*p)) {
      p++;
      continue;
    }
    if (*p == '\'') {
      p++;
      start = p;
      while (*p != '\0' && *p != '\'') {
        p++;
      }
      if (*p != '\'') {
        set_err(err, "unterminated string literal");
        tokens_free(toks, count);
        return 0;
      }
      if (!token_push(&toks, &count, &cap, start, (size_t)(p - start), 1)) {
        set_err(err, "out of memory");
        tokens_free(toks, count);
        return 0;
      }
      p++;
      continue;
    }

    if (*p == ',' || *p == '(' || *p == ')' || *p == ';' || *p == '*') {
      if (!token_push(&toks, &count, &cap, p, 1, 0)) {
        set_err(err, "out of memory");
        tokens_free(toks, count);
        return 0;
      }
      p++;
      continue;
    }

    if (*p == '<' || *p == '>' || *p == '=' || *p == '!') {
      if ((*p == '<' || *p == '>' || *p == '!') && *(p + 1) == '=') {
        if (!token_push(&toks, &count, &cap, p, 2, 0)) {
          set_err(err, "out of memory");
          tokens_free(toks, count);
          return 0;
        }
        p += 2;
      } else {
        if (!token_push(&toks, &count, &cap, p, 1, 0)) {
          set_err(err, "out of memory");
          tokens_free(toks, count);
          return 0;
        }
        p++;
      }
      continue;
    }

    start = p;
    while (*p != '\0' && !isspace((unsigned char)*p) && *p != ',' && *p != '(' && *p != ')' &&
           *p != ';' && *p != '*' && *p != '<' && *p != '>' && *p != '=' && *p != '!' && *p != '\'') {
      p++;
    }
    if (!token_push(&toks, &count, &cap, start, (size_t)(p - start), 0)) {
      set_err(err, "out of memory");
      tokens_free(toks, count);
      return 0;
    }
  }

  *out_toks = toks;
  *out_count = count;
  return 1;
}

static int ieq_tok(const token* toks, size_t i, size_t n, const char* s) {
  if (i >= n) {
    return 0;
  }
  return fx_ieq(toks[i].text, s);
}

static int push_str(char*** arr, size_t* count, size_t* cap, const char* s) {
  if (*count == *cap) {
    size_t ncap = *cap == 0 ? 8 : *cap * 2;
    char** ns = (char**)realloc(*arr, ncap * sizeof(char*));
    if (ns == NULL) {
      return 0;
    }
    *arr = ns;
    *cap = ncap;
  }
  (*arr)[*count] = fx_strdup(s);
  if ((*arr)[*count] == NULL) {
    return 0;
  }
  *count += 1;
  return 1;
}

static void free_condition(flexql_condition* c) {
  free(c->left);
  free(c->op);
  free(c->right);
  memset(c, 0, sizeof(*c));
}

void parser_free_statement(flexql_statement* stmt) {
  size_t i;
  size_t j;
  if (stmt == NULL) {
    return;
  }

  switch (stmt->type) {
    case FLEXQL_STMT_CREATE:
      free(stmt->as.create_stmt.table_name);
      for (i = 0; i < stmt->as.create_stmt.col_count; ++i) {
        free(stmt->as.create_stmt.col_names[i]);
        free(stmt->as.create_stmt.col_types[i]);
      }
      free(stmt->as.create_stmt.col_names);
      free(stmt->as.create_stmt.col_types);
      break;
    case FLEXQL_STMT_INSERT:
      free(stmt->as.insert_stmt.table_name);
      for (i = 0; i < stmt->as.insert_stmt.row_count; ++i) {
        for (j = 0; j < stmt->as.insert_stmt.value_count; ++j) {
          free(stmt->as.insert_stmt.rows[i][j]);
        }
        free(stmt->as.insert_stmt.rows[i]);
      }
      free(stmt->as.insert_stmt.rows);
      break;
    case FLEXQL_STMT_SELECT:
      free(stmt->as.select_stmt.from_table);
      free(stmt->as.select_stmt.join_table);
      free(stmt->as.select_stmt.order_by);
      for (i = 0; i < stmt->as.select_stmt.select_count; ++i) {
        free(stmt->as.select_stmt.select_items[i]);
      }
      free(stmt->as.select_stmt.select_items);
      free_condition(&stmt->as.select_stmt.where_cond);
      free_condition(&stmt->as.select_stmt.join_cond);
      break;
    case FLEXQL_STMT_DELETE:
      free(stmt->as.delete_stmt.table_name);
      break;
    case FLEXQL_STMT_RESET:
      free(stmt->as.reset_stmt.table_name);
      break;
    default:
      break;
  }
  memset(stmt, 0, sizeof(*stmt));
}

static int parse_condition(const token* toks, size_t* i, size_t n, flexql_condition* out,
                           char** err) {
  long double parsed;
  if (*i + 2 >= n) {
    set_err(err, "incomplete condition");
    return 0;
  }
  out->left = fx_strdup(toks[*i].text);
  out->op = fx_strdup(toks[*i + 1].text);
  out->right = fx_strdup(toks[*i + 2].text);
  if (out->left == NULL || out->op == NULL || out->right == NULL) {
    set_err(err, "out of memory");
    return 0;
  }
  if (!is_cmp_op(out->op)) {
    set_err(err, "expected comparison operator (=, >, <, >=, <=, !=)");
    return 0;
  }
  out->right_quoted = toks[*i + 2].quoted;
  out->right_is_literal = toks[*i + 2].quoted || strchr(out->right, '.') == NULL;
  if (fx_parse_number(out->right, &parsed)) {
    out->right_is_literal = 1;
  }
  *i += 3;
  return 1;
}

static int parse_create(const token* toks, size_t n, flexql_statement* st, char** err) {
  size_t i = 1;
  size_t cap = 0;
  if (!ieq_tok(toks, i, n, "TABLE")) {
    set_err(err, "expected TABLE");
    return 0;
  }
  i++;

  if (ieq_tok(toks, i, n, "IF") && ieq_tok(toks, i + 1, n, "NOT") && ieq_tok(toks, i + 2, n, "EXISTS")) {
    st->as.create_stmt.if_not_exists = 1;
    i += 3;
  }

  if (i >= n) {
    set_err(err, "expected table name");
    return 0;
  }
  st->as.create_stmt.table_name = fx_strdup(toks[i].text);
  if (st->as.create_stmt.table_name == NULL) {
    set_err(err, "out of memory");
    return 0;
  }
  i++;

  if (i >= n || strcmp(toks[i].text, "(") != 0) {
    set_err(err, "expected (");
    return 0;
  }
  i++;

  while (i < n && strcmp(toks[i].text, ")") != 0) {
    char* col_name;
    char* col_type;
    char** new_names;
    char** new_types;
    if (i + 1 >= n) {
      set_err(err, "expected column definition");
      return 0;
    }
    col_name = fx_strdup(toks[i].text);
    i++;

    col_type = fx_strdup(toks[i].text);
    i++;

    if (i + 2 < n && strcmp(toks[i].text, "(") == 0 && strcmp(toks[i + 2].text, ")") == 0) {
      size_t base_len = strlen(col_type);
      size_t arg_len = strlen(toks[i + 1].text);
      char* merged = (char*)malloc(base_len + arg_len + 3);
      if (merged == NULL) {
        set_err(err, "out of memory");
        return 0;
      }
      memcpy(merged, col_type, base_len);
      merged[base_len] = '(';
      memcpy(merged + base_len + 1, toks[i + 1].text, arg_len);
      merged[base_len + arg_len + 1] = ')';
      merged[base_len + arg_len + 2] = '\0';
      free(col_type);
      col_type = merged;
      i += 3;
    }

    if (st->as.create_stmt.col_count == cap) {
      size_t ncap = cap == 0 ? 8 : cap * 2;
      new_names = (char**)realloc(st->as.create_stmt.col_names, ncap * sizeof(char*));
      new_types = (char**)realloc(st->as.create_stmt.col_types, ncap * sizeof(char*));
      if (new_names == NULL || new_types == NULL) {
        free(new_names);
        free(new_types);
        free(col_name);
        free(col_type);
        set_err(err, "out of memory");
        return 0;
      }
      st->as.create_stmt.col_names = new_names;
      st->as.create_stmt.col_types = new_types;
      cap = ncap;
    }

    st->as.create_stmt.col_names[st->as.create_stmt.col_count] = col_name;
    st->as.create_stmt.col_types[st->as.create_stmt.col_count] = col_type;
    st->as.create_stmt.col_count += 1;

    if (i < n && strcmp(toks[i].text, ",") == 0) {
      i++;
    }
  }

  if (i >= n || strcmp(toks[i].text, ")") != 0) {
    set_err(err, "expected )");
    return 0;
  }

  return st->as.create_stmt.col_count > 0;
}

static int parse_insert(const token* toks, size_t n, flexql_statement* st, char** err) {
  size_t i = 1;
  if (!ieq_tok(toks, i, n, "INTO")) {
    set_err(err, "expected INTO");
    return 0;
  }
  i++;
  if (i >= n) {
    set_err(err, "expected table name");
    return 0;
  }
  st->as.insert_stmt.table_name = fx_strdup(toks[i].text);
  if (st->as.insert_stmt.table_name == NULL) {
    set_err(err, "out of memory");
    return 0;
  }
  i++;

  if (!ieq_tok(toks, i, n, "VALUES")) {
    set_err(err, "expected VALUES");
    return 0;
  }
  i++;

  while (i < n) {
    size_t value_count = 0;
    size_t value_cap = 0;
    char** values = NULL;
    char*** new_rows;

    if (strcmp(toks[i].text, ";") == 0) {
      break;
    }
    if (strcmp(toks[i].text, "(") != 0) {
      set_err(err, "expected (");
      return 0;
    }
    i++;

    while (i < n && strcmp(toks[i].text, ")") != 0) {
      if (strcmp(toks[i].text, ",") == 0) {
        i++;
        continue;
      }
      if (!push_str(&values, &value_count, &value_cap, toks[i].text)) {
        set_err(err, "out of memory");
        return 0;
      }
      i++;
    }

    if (i >= n || strcmp(toks[i].text, ")") != 0) {
      set_err(err, "expected ) in VALUES");
      return 0;
    }
    i++;

    if (st->as.insert_stmt.value_count == 0) {
      st->as.insert_stmt.value_count = value_count;
    }

    new_rows = (char***)realloc(st->as.insert_stmt.rows,
                                (st->as.insert_stmt.row_count + 1) * sizeof(char**));
    if (new_rows == NULL) {
      set_err(err, "out of memory");
      return 0;
    }
    st->as.insert_stmt.rows = new_rows;
    st->as.insert_stmt.rows[st->as.insert_stmt.row_count] = values;
    st->as.insert_stmt.row_count += 1;

    if (i < n && strcmp(toks[i].text, ",") == 0) {
      i++;
    }
  }

  return st->as.insert_stmt.row_count > 0;
}


static int parse_select(const token* toks, size_t n, flexql_statement* st, char** err) {
  size_t i = 1;
  size_t cap = 0;

  if (i < n && strcmp(toks[i].text, "*") == 0) {
    st->as.select_stmt.select_all = 1;
    i++;
  } else {
    while (i < n && !ieq_tok(toks, i, n, "FROM")) {
      if (strcmp(toks[i].text, ",") != 0) {
        if (!push_str(&st->as.select_stmt.select_items, &st->as.select_stmt.select_count, &cap,
                      toks[i].text)) {
          set_err(err, "out of memory");
          return 0;
        }
      }
      i++;
    }
  }

  if (!ieq_tok(toks, i, n, "FROM")) {
    set_err(err, "expected FROM");
    return 0;
  }
  i++;

  if (i >= n) {
    set_err(err, "expected table name after FROM");
    return 0;
  }
  st->as.select_stmt.from_table = fx_strdup(toks[i].text);
  if (st->as.select_stmt.from_table == NULL) {
    set_err(err, "out of memory");
    return 0;
  }
  i++;

  if (ieq_tok(toks, i, n, "INNER") && ieq_tok(toks, i + 1, n, "JOIN")) {
    st->as.select_stmt.has_join = 1;
    i += 2;
    if (i >= n) {
      set_err(err, "expected table name after JOIN");
      return 0;
    }
    st->as.select_stmt.join_table = fx_strdup(toks[i].text);
    if (st->as.select_stmt.join_table == NULL) {
      set_err(err, "out of memory");
      return 0;
    }
    i++;

    if (!ieq_tok(toks, i, n, "ON")) {
      set_err(err, "expected ON");
      return 0;
    }
    i++;

    if (!parse_condition(toks, &i, n, &st->as.select_stmt.join_cond, err)) {
      return 0;
    }
    st->as.select_stmt.join_cond.right_is_literal = 0;
  }

  if (ieq_tok(toks, i, n, "WHERE")) {
    st->as.select_stmt.has_where = 1;
    i++;
    if (!parse_condition(toks, &i, n, &st->as.select_stmt.where_cond, err)) {
      return 0;
    }
  }

  if (ieq_tok(toks, i, n, "ORDER") && ieq_tok(toks, i + 1, n, "BY")) {
    st->as.select_stmt.has_order_by = 1;
    i += 2;
    if (i >= n) {
      set_err(err, "expected ORDER BY column");
      return 0;
    }
    st->as.select_stmt.order_by = fx_strdup(toks[i].text);
    if (st->as.select_stmt.order_by == NULL) {
      set_err(err, "out of memory");
      return 0;
    }
    i++;
  }

  return 1;
}

int parser_parse_sql(const char* sql, flexql_statement* stmt, char** err) {
  token* toks = NULL;
  size_t n = 0;
  int ok = 0;

  memset(stmt, 0, sizeof(*stmt));

  if (!tokenize(sql, &toks, &n, err)) {
    return 0;
  }
  if (n == 0) {
    tokens_free(toks, n);
    stmt->type = FLEXQL_STMT_NONE;
    return 1;
  }

  if (fx_ieq(toks[0].text, "CREATE")) {
    stmt->type = FLEXQL_STMT_CREATE;
    ok = parse_create(toks, n, stmt, err);
  } else if (fx_ieq(toks[0].text, "INSERT")) {
    stmt->type = FLEXQL_STMT_INSERT;
    ok = parse_insert(toks, n, stmt, err);
  } else if (fx_ieq(toks[0].text, "SELECT")) {
    stmt->type = FLEXQL_STMT_SELECT;
    ok = parse_select(toks, n, stmt, err);
  } else if (fx_ieq(toks[0].text, "DELETE")) {
    stmt->type = FLEXQL_STMT_DELETE;
    if (n >= 3 && fx_ieq(toks[1].text, "FROM")) {
      stmt->as.delete_stmt.table_name = fx_strdup(toks[2].text);
      ok = stmt->as.delete_stmt.table_name != NULL;
    } else {
      set_err(err, "expected DELETE FROM <table>");
    }
  } else if (fx_ieq(toks[0].text, "RESET")) {
    stmt->type = FLEXQL_STMT_RESET;
    if (n >= 2) {
      stmt->as.reset_stmt.table_name = fx_strdup(toks[1].text);
      ok = stmt->as.reset_stmt.table_name != NULL;
    } else {
      set_err(err, "expected RESET <table>");
    }
  } else {
    set_err(err, "unsupported SQL command");
  }

  tokens_free(toks, n);

  if (!ok) {
    parser_free_statement(stmt);
    return 0;
  }
  return 1;
}
