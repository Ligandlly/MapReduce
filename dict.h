#ifndef DICT_H
#define DICT_H
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "list.h"

struct row
{
  char *key;
  struct list_head row_head;
  struct list_head row_list;
  pthread_mutex_t mutex;
};

struct cell
{
  char *key;
  char *value;
  struct list_head cell_list;
};

struct row *create_row(char *key);
struct row *get_row(char *key, struct list_head *head);
struct cell *create_cell(char *key, char *value);
void add_cell(char *key, char *value, struct list_head *dict, pthread_mutex_t *global_mutex);
void display_dict(struct list_head *head);

#endif