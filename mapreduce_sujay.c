#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

struct kvpair
{
	char *key;
	char *value;
	int valid;
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
            p= next_partition++;
            pthread_mutex_unlock(&part_lock);
            ptr = part[p];
		    while(ptr != NULL) {
			    iter = ptr;
			    fp_reduce(ptr->key, get_next, p);
		    	ptr = ptr->next;
			    while(ptr != NULL) {
			    	if(strcmp(ptr->key, iter->key) != 0) {
			    		break;
			    	}
				    ptr = ptr->next;
			    }
		    }   	
	    } else {
            pthread_mutex_unlock(&part_lock);
        }
    }
}

void MR_Emit(char *key, char *value)
{
	struct kvpair *new = malloc(sizeof(struct kvpair));
	new->key = strdup(key);
	new->value = strdup(value);
	new->valid = 1;

	int p = fp_part(key, n_partitions);

	struct kvpair *ptr = part[p];
	struct kvpair *prev;

	pthread_mutex_lock(&part_lock);
    if(ptr == NULL) {
		new->next = NULL;
		part[p] = new;
		goto end;
	}

	while(ptr != NULL && (strcmp(ptr->key, key) < 0)) {
		prev = ptr;
		ptr = ptr->next;
	}
	new->next = ptr;
	if(prev != NULL)
		prev->next = new;
	else
		part[p] = new;

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

	if(strcmp(ptr->key, key) == 0 && ptr->valid) {
		ptr->valid = 0;
		return ptr->value;
	}

	ptr = ptr->next;
	while(ptr != NULL) {
		if(strcmp(ptr->key, key) == 0 && ptr->valid) {
			ptr->valid = 0;
			return ptr->value;
		}
		ptr = ptr->next;
	}

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

    part = (struct kvpair *)malloc(n_partitions * sizeof(struct kvpair *));

    for(i=0; i<num_mappers; i++) {
    	pthread_create(&mthread[i], NULL, (void *)map_pool, NULL);
    }
    for(i=0; i<num_mappers; i++) {
        pthread_join(mthread[i], NULL);
    }

    for(i=0; i<num_reducers; i++) {
    	pthread_create(&rthread[i], NULL, (void *)reduce_pool, NULL);
    }
    for(i=0; i<num_mappers; i++) {
        pthread_join(rthread[i], NULL);
    }

    for(i=0; i<n_partitions; i++) {
        struct kvpair *ptr = part[i];
        struct kvpair *iter;
        
        while(ptr != NULL) {
            iter = ptr;
            ptr = ptr->next;
            free(iter->key);
            free(iter->value);
            free(iter);
        }
    }
}
