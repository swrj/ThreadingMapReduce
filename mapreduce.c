#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <string.h>
#include "mapreduce.h"

Partitioner partitioner;
Mapper threadmap;
Reducer threadreduce;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t c  = PTHREAD_COND_INITIALIZER;

typedef struct{
        char *key;
        char *value;
        int done;

}KeyValue;

KeyValue **kv;
int pnumbers[100];
int Numreducers;
int reducecounter[100];
int pnumber = -1;

char * MyGetter(char *key, int partition_number){
        int r = reducecounter[partition_number];
        if(kv[partition_number][r].done == 1){
                r++;
                reducecounter[partition_number] = r;
                return NULL;
        }
        else{
                if(r < pnumbers[partition_number] -1  &&
                        strcmp(kv[partition_number][r].key, kv[partition_number][r+1].key) != 0){
                                kv[partition_number][r].done = 1;
                                return kv[partition_number][r].value;
                }
                else{
                        r++;
                        reducecounter[partition_number] = r;
                        return kv[partition_number][r-1].value;
                }

        }
}

void * mapthread(void* arg){
        threadmap(arg);
        return NULL;
}

int cmpstr(const void *p1, const void *p2){
        return strcmp(* (char * const *) p1, * (char * const *) p2);
}

void * reducethread(void *arg){
        pthread_mutex_lock(&lock);
        pnumber++;
        pthread_mutex_unlock(&lock);
        int arg2 = pnumber-1;
        qsort(kv[arg2], pnumbers[arg2], sizeof(kv[arg2][0]), cmpstr);
        Getter get = malloc(sizeof(void*));
        get = MyGetter;
        int i = 0;
        while(i<pnumbers[arg2]){
                threadreduce(kv[arg2][i].key ,get ,arg2);
                while(i < pnumbers[arg2] -1  && strcmp(kv[arg2][i].key, kv[arg2][i+1].key) == 0){
                                i++;
                }
                i++;
        }
        return NULL;
}


void MR_Run(int argc, char *argv[],
                Mapper map, int num_mappers,
                Reducer reduce, int num_reducers,
                Partitioner partition){

        pnumber = 0;
        int nprocs = get_nprocs();
        int mapthreadsize;
        int reducethreadsize;
        Numreducers = num_reducers;
        memset(pnumbers, 0, 100 * sizeof(pnumbers[0]));
        memset(reducecounter, 0, 100 * sizeof(reducecounter[0]));
        if(nprocs > num_mappers){
                mapthreadsize = num_mappers;
        }
        else{
                mapthreadsize = nprocs;
        }
        if(nprocs > num_reducers){
                reducethreadsize = num_reducers;
        }
        else{
                reducethreadsize = nprocs;
        }
        pthread_t maparray[mapthreadsize];
        pthread_t reducearray[reducethreadsize];
        int i;
        kv = (KeyValue **) malloc(sizeof(KeyValue *) * 50);
        for(i=0; i<50; i++){
                kv[i] = (KeyValue *) malloc(sizeof(KeyValue) * 500000000);
        }

        threadmap = map;
        threadreduce = reduce;
        partitioner = partition;

        int k = 0;
        for(i=0; i<argc-1; i++){
                if(k<mapthreadsize){
                        pthread_create(&maparray[k], NULL, mapthread, (void*)argv[i+1]);
                        k++;
                }
                else{
                        k = 0;
                        i--;
                        int j;
                        for(j=0; j<mapthreadsize; j++){
                                pthread_join(maparray[j], NULL);
                        }
                }
        }

        for(i=0; i<k; i++){
                pthread_join(maparray[i], NULL);
        }

        k = 0;
        for(i=0; i<num_reducers+1; i++){
                if(k<reducethreadsize){
                        if(pnumbers[pnumber] != 0){
                                pthread_create(&reducearray[k], NULL, reducethread, NULL);
                                k++;
                        }
                        else{
                                pnumber++;
                        }
                }
                else{
                        k = 0;
                        int j;
                        i--;
                        for(j=0; j<reducethreadsize; j++){
                                pthread_join(reducearray[j], NULL);
                        }
                }
        }

        for(i=0; i<k; i++){
                pthread_join(reducearray[i], NULL);
        }
        for(i=0; i<50; i++){
                free(kv[i]);
        }
}

void MR_Emit(char *key, char *value){
        unsigned long hash;
        hash = partitioner(key, Numreducers);
        pthread_mutex_lock(&lock);
        kv[hash][pnumbers[hash]].key = malloc(strlen(key)+1);
        strcpy(kv[hash][pnumbers[hash]].key, key);
        kv[hash][pnumbers[hash]].value = malloc(strlen(value) +1);
        strcpy(kv[hash][pnumbers[hash]].value, value);
        kv[hash][pnumbers[hash]].done = 0;
        pnumbers[hash]++;
        pthread_mutex_unlock(&lock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
        unsigned long hash = 5381;
        int c;
        while ((c = *key++) != '\0')
                hash = hash * 33 + c;
        return hash % num_partitions;
}
//int main(int argc, char *argv[]){
//return 0;
//}
