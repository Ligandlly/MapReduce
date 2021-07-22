#include "dict.h"

static void _strcpy_calloc(char **dist, char *const src)
{
	*dist = calloc(sizeof(char), strlen(src) + 1);
	strcpy(*dist, src);
}

struct row *create_row(char *const key)
{
	struct row *result = malloc(sizeof(struct row));
	_strcpy_calloc(&result->key, key);

	INIT_LIST_HEAD(&(result->row_head));
	INIT_LIST_HEAD(&(result->row_list));
	pthread_mutex_init(&(result->mutex), NULL);
	return result;
}

/**
 * Find a row by key
 * @param key
 * @param head The dictionary
 * @return struct row* found by key or null if not found
 */
struct row *get_row(char *const key, struct list_head *head)
{
	struct list_head *iter = NULL;

	__list_for_each(iter, head)
	{
		struct row *row_ptr = list_entry(iter, struct row, row_list);
		if (strcmp(row_ptr->key, key) == 0)
		{
			return row_ptr;
		}
	}
	return NULL;
}

/**
 * Create a cell struct and return its pointer
 * @param key
 * @param value
 * @return a pointer to struct cell
 */
struct cell *create_cell(char *const key, char *const value)
{
	struct cell *tmp_cell = malloc(sizeof(struct cell));
	_strcpy_calloc(&tmp_cell->key, key);
	_strcpy_calloc(&tmp_cell->value, value);

	INIT_LIST_HEAD(&tmp_cell->cell_list);

	return tmp_cell;
}

/**
 * Add a cell to dict
 * @param key
 * @param value
 * @param dict
 */
void add_cell(char *const key, char *const value, struct list_head *const dict, pthread_mutex_t *global_mutex)
{
	struct cell *tmp_cell = create_cell(key, value);
	struct row *row_got = get_row(key, dict);
	if (row_got == NULL)
	{
		row_got = create_row(key);
		pthread_mutex_lock(global_mutex);
		list_add_tail(&(row_got->row_list), dict);
		pthread_mutex_unlock(global_mutex);
	}
	pthread_mutex_lock((&row_got->mutex));
	list_add_tail(&(tmp_cell->cell_list), &(row_got->row_head));
	pthread_mutex_unlock((&row_got->mutex));
}

void display_dict(struct list_head *const head)
{
	struct list_head *iter = NULL;
	__list_for_each(iter, head)
	{
		struct row *each_row =
				list_entry(iter, struct row, row_list);
		printf("Row: %s:\n", each_row->key);

		struct list_head *cell_iter = NULL;
		__list_for_each(cell_iter, &(each_row->row_head))
		{
			struct cell *each_cell =
					list_entry(cell_iter, struct cell, cell_list);
			printf("%s\t%s\n", each_cell->key, each_cell->value);
		}
	}
}

void free_cell(struct cell *ptr)
{
	free(ptr->value);
	free(ptr->key);
	free(ptr);
}

void free_row(struct row *ptr)
{
	free(ptr->key);
	pthread_mutex_destroy(&ptr->mutex);
	struct list_head *iter;
	__list_for_each(iter, &ptr->row_head)
	{
		struct cell *item = list_entry(iter, struct cell, cell_list);
		free_cell(item);
	}
}