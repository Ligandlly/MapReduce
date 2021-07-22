#include <stdlib.h>
#include "mapreduce.h"
#include "string.h"
#include "dict.h"

struct list_head global_dict;
pthread_mutex_t global_dict_mutex;
struct list_head *reducer_data_list;

void MR_Emit(char *key, char *value)
{
  add_cell(key, value, &global_dict, &global_dict_mutex);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

struct map_arg
{
  Mapper map;
  char *file_name;
};

void *map_wrap(void *arg)
{
  struct map_arg *mapper_arg = (struct map_arg *)arg;
  mapper_arg->map(mapper_arg->file_name);

  pthread_exit(EXIT_SUCCESS);
}

struct reduce_arg
{
  Reducer reducer;
  Getter get_func;
  int partition_number;
};

void *reduce_wrap(void *arg)
{
  struct reduce_arg *reduce_arg = (struct reduce_arg *)arg;
  struct list_head *head = reducer_data_list + reduce_arg->partition_number;
  while (!list_empty(head))
  {
    struct cell *item = list_entry(head->next, struct cell, cell_list);
    reduce_arg->reducer(item->key, reduce_arg->get_func, reduce_arg->partition_number);
  }

  pthread_exit(EXIT_SUCCESS);
}

void hash_partition(struct list_head *src, struct list_head dist[], Partitioner partitioner, int num_partitions)
{
  struct list_head *iter = NULL;
  __list_for_each(iter, src)
  {
    struct row *current_row = list_entry(iter, struct row, row_list);
    size_t hash = partitioner(current_row->key, num_partitions);
    list_splice_init(&current_row->row_head, dist + hash);
  }
}

char *get_next(char *key, int partition_number)
{
  struct list_head *current = reducer_data_list + partition_number;
  if (list_empty(current))
  {
    return NULL;
  }

  struct list_head *iter;
  __list_for_each(iter, current)
  {
    struct cell *item = list_entry(iter, struct cell, cell_list);
    if (strcmp(item->key, key) == 0)
    {
      char *result = item->value;
      list_del(iter);
      return result;
    }
  }
  return NULL;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{
  pthread_mutex_init(&global_dict_mutex, NULL);
  INIT_LIST_HEAD(&global_dict);

  pthread_t map_thread_ids[num_mappers];
  struct map_arg mapper_args[num_mappers];
  for (int i = 0; i < num_mappers; ++i)
  {
    mapper_args[i].map = map;
    mapper_args[i].file_name = argv[i + 1];
    pthread_create(map_thread_ids + i, NULL, map_wrap, mapper_args + i);
  }

  for (int i = 0; i < num_mappers; ++i)
  {
    pthread_join(map_thread_ids[i], NULL);
  }
  reducer_data_list = (struct list_head *)calloc(sizeof(struct list_head), num_reducers);
  for (int i = 0; i < num_reducers; ++i)
  {
    INIT_LIST_HEAD(reducer_data_list + i);
  }

  hash_partition(&global_dict, reducer_data_list, partition, num_reducers);

  pthread_t reduce_thread_ids[num_reducers];
  struct reduce_arg reduce_args[num_reducers];
  for (int i = 0; i < num_reducers; ++i)
  {
    reduce_args[i].reducer = reduce;
    reduce_args[i].partition_number = i;
    reduce_args[i].get_func = get_next;
    pthread_create(reduce_thread_ids + i, NULL, reduce_wrap, reduce_args + i);
  }
  for (int i = 0; i < num_reducers; ++i)
  {
    pthread_join(reduce_thread_ids[i], NULL);
  }

  free(reducer_data_list);

  struct list_head *iter;
  __list_for_each(iter, &global_dict)
  {
    struct row *row = list_entry(iter, struct row, row_list);
    free(row->key);
    free(row);
    pthread_mutex_destroy(&row->mutex);
  }
  pthread_mutex_destroy(&global_dict_mutex);
}