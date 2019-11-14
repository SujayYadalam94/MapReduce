#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

typedef struct {
	char *key;
	Getter fp;
	int partition_number;
} myarg_t;

struct kvpair
{
	char *key;
	char *value;
	int valid;
	struct kvpair *next;
};

Reducer fp_reduce;
Partitioner fp_part;

struct kvpair *part[10];

int n_partitions;

void MR_Emit(char *key, char *value)
{
	struct kvpair *new = malloc(sizeof(struct kvpair));
	new->key = strdup(key);
	new->value = strdup(value);
	new->valid = 1;

	int p = fp_part(key, n_partitions);

	struct kvpair *ptr = part[p];
	struct kvpair *prev;

	if(ptr == NULL) {
		new->next = NULL;
		part[p] = new;
		return;
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

void dummy(void)
{
	struct kvpair *ptr, *iter;
	for(int i=0; i<n_partitions; i++) {
		ptr = part[i];
		while(ptr != NULL) {
			iter = ptr;
			fp_reduce(ptr->key, get_next, i);
			ptr = ptr->next;
			while(ptr != NULL) {
				if(strcmp(ptr->key, iter->key) != 0) {
					break;
				}
				ptr = ptr->next;
			}
		}	
	}
}


void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition, int num_partitions)
{
	pthread_t mthread, rthread;

	n_partitions = num_partitions;

	fp_reduce = reduce;
	fp_part = partition;

	pthread_create(&mthread, NULL, (void *)map, argv[1]);
	pthread_join(mthread, NULL);

	pthread_create(&rthread, NULL, (void *)dummy, NULL);
	pthread_join(rthread, NULL);
}
