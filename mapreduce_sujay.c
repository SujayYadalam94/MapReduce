#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

struct node {
	char key[128]; // TODO: need to change this
	char value[16];
	struct node *next;
};

struct node **part_table;

char **files;
int partitions;

int next_file = 0;
int next_partition = 0;

pthread_mutex_t map_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t reduce_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t part_lock = PTHREAD_MUTEX_INITIALIZER;

Partitioner fp_part;
Mapper 		fp_map;
Reducer 	fp_reduce;

void *map_worker()
{
	char *file;
		
	while(files[next_file]) {
		pthread_mutex_lock(&map_lock);
		if(files[next_file]) {
			file = files[next_file++];
			pthread_mutex_unlock(&map_lock);
			fp_map(file);
		} else {
			pthread_mutex_unlock(&map_lock);
		}
	}
}

void *reduce_worker()
{
	int np;
	while(next_partition < partitions-1) {
		pthread_mutex_lock(&reduce_lock);
		if(next_partition < partitions-1) {
			np = next_partition++;
			pthread_mutex_unlock(&reduce_lock);
			fp_reduce(part_table[np]->key, get_next, np); //need to verify this
		} else {
			pthread_mutex_unlock(&reduce_lock);
		}
	}
}

char* get_next(char *key, int partition_number)
{
	char *ret;
	struct node *ptr;

	pthread_mutex_lock(&part_lock);
	ptr = part_table[partition_number];
	if(ptr == NULL) {
		ret = NULL;
	}
	else {
		ret = ptr->value;
		part_table[partition_number] = ptr->next;
		free(ptr);
	}
	pthread_mutex_unlock(&part_lock);

	return ret;
}

void MR_Emit(char *key, char *value)
{
	struct node *new, *ptr;

	new = malloc(sizeof(struct node));
	strcpy(new->value, value);
	strcpy(new->key, key);
	new->next = NULL;
	
	// Need to acquire lock to modify hashtable
	pthread_mutex_lock(&part_lock);
	ptr = part_table[fp_part(key, partitions)];
	if(ptr == NULL) {
		ptr = new;
	} else {
		while (ptr->next != NULL) {
			ptr = ptr->next;
		}
		ptr->next = new;
	}
	// Release hash table lock
	pthread_mutex_unlock(&part_lock);
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

}

void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition, int num_partitions)
{
	int i;
	pthread_t map_threads[num_mappers];
	pthread_t reduce_threads[num_reducers];
	files = argv;
	partitions = num_partitions;
	part_table = malloc(partitions * sizeof(struct node*));

	fp_map = map;
	fp_reduce = reduce;
	fp_part = partition;

	for(i=0; i<num_mappers; i++) {
		pthread_create(&(map_threads[i]), NULL, map_worker, NULL);
	}
	for(i=0; i<num_mappers; i++) {
		pthread_join(map_threads[i], NULL);
	}

	for(i=0; i<num_reducers; i++) {
		pthread_create(&(reduce_threads[i]), NULL, reduce_worker, NULL);
	}
	for(i=0; i<num_reducers; i++) {
		pthread_join(reduce_threads[i], NULL);
	}

	free(part_table);
}
