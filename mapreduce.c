#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

struct vnode
{
	char *value;
	struct vnode *next;
};

struct kvpair
{
	char *key;
	struct vnode *vn;
	struct vnode *current;
	struct kvpair *next;
};

Mapper fp_map;
Reducer fp_reduce;
Partitioner fp_part;

struct kvpair **part;
char **files;
int n_partitions;
int next_file = 1; // Files start from argv[1]
int next_partition = 0;

pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t part_lock = PTHREAD_MUTEX_INITIALIZER;

void map_pool(void* args)
{
    char *file;

    while(files[next_file] != NULL && strlen(files[next_file]) > 0) {
        pthread_mutex_lock(&file_lock);
        if(files[next_file]) {
            file = files[next_file++];
            pthread_mutex_unlock(&file_lock);
            fp_map(file);
        } else {
            pthread_mutex_unlock(&file_lock);
        }
    }
}

void reduce_pool(void* args)
{
	struct kvpair *ptr, *iter;
    int p;
	
    while(next_partition < n_partitions) {
        pthread_mutex_lock(&part_lock);
        if(next_partition < n_partitions) {    	
            p = next_partition++;
            pthread_mutex_unlock(&part_lock);
            ptr = part[p];
		    while(ptr != NULL) {
			    fp_reduce(ptr->key, get_next, p);
		    	ptr = ptr->next;
			    } 	
	    } else {
            pthread_mutex_unlock(&part_lock);
        }
    }
}

void insert_value(struct kvpair *key_ptr, char *value)
{
	struct vnode *ptr, *prev = NULL, *new;
	new = malloc(sizeof(struct vnode));
	new->value = strdup(value);
	new->next = NULL;

	ptr = key_ptr->vn;

	if(ptr == NULL) {
		key_ptr->vn = new;
		key_ptr->current = new;
		return;
	}

	while(ptr != NULL && (strcmp(ptr->value, value) < 0)) {
		prev = ptr;
		ptr = ptr->next;
	}
	new->next = ptr;
	if(prev != NULL)
		prev->next = new;
	else {
		key_ptr->vn = new;
		key_ptr->current = new;
	}
}

void MR_Emit(char *key, char *value)
{
	struct kvpair *ptr, *prev=NULL, *new;
	int p;

	p = fp_part(key, n_partitions);

	pthread_mutex_lock(&part_lock);
	ptr = part[p];
	if(ptr == NULL) {
		new = malloc(sizeof(struct kvpair));
		new->key = strdup(key);
		new->next = NULL;
		insert_value(new, value);

		part[p] = new;
		goto end;
	}

	while(ptr != NULL) {
		int cmp = strcmp(ptr->key, key);
		if(cmp == 0) {
			insert_value(ptr, value);
			goto end;
		} else if(cmp > 0) {
			break;
		}
		prev = ptr;
		ptr = ptr->next;
	}

	new = malloc(sizeof(struct kvpair));
	new->key = strdup(key);
	new->next = ptr;
	if(prev != NULL)
		prev->next = new;
	else
		part[p] = new;
	insert_value(new, value);

end:
	pthread_mutex_unlock(&part_lock);
}

char* get_next(char *key, int partition_number)
{
	struct kvpair *ptr;
	char *temp;

	ptr = part[partition_number];
	if (ptr == NULL)
		return NULL;
	
	while(ptr != NULL) {
		if(strcmp(ptr->key, key) == 0) {
			
			if(ptr->current == NULL)
				goto end;
			temp = ptr->current->value;
			ptr->current = ptr->current->next;
			return temp;
		}
		ptr = ptr->next;
	}

end:
	return NULL;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions)
{
	unsigned long temp = atoi(key);
	temp = temp & 0x00000000ffffffff;
	temp = temp >> (32 - num_partitions/2);
	return temp;
}

void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition, int num_partitions)
{
    int i;

	pthread_t mthread[num_mappers], rthread[num_reducers];

	n_partitions = num_partitions;
    files = argv;

    fp_map = map;
	fp_reduce = reduce;
	fp_part = partition;

    part = (struct kvpair **)malloc(n_partitions * sizeof(struct kvpair *));

    for(i=0; i<num_mappers; i++) {
    	pthread_create(&mthread[i], NULL, (void *)map_pool, NULL);
    }
    for(i=0; i<num_mappers; i++) {
        pthread_join(mthread[i], NULL);
    }

    for(i=0; i<num_reducers; i++) {
    	pthread_create(&rthread[i], NULL, (void *)reduce_pool, NULL);
    }
    for(i=0; i<num_reducers; i++) {
        pthread_join(rthread[i], NULL);
    }

    for(i=0; i<n_partitions; i++) {
        struct kvpair *ptr = part[i];
        struct kvpair *iter;
        
        while(ptr != NULL) {
            struct vnode *temp;
            while(ptr->vn) {
            	temp = ptr->vn;
            	ptr->vn = ptr->vn->next;
            	free(temp->value);
            	free(temp);
            }
            iter = ptr;
            ptr = ptr->next;
            free(iter);
        }
    }
}
