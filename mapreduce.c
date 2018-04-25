#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <string.h>
#include "mapreduce.h"
// External functions: these are wh

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
// Counts number of elements in each partition
int pnumbers[100];
int Numreducers = 5;
// counts where its at with getter for each partition
int reducecounter[100];
// counts what partition we're on
int pnumber = -1;
// my lock thing

//todo list: make thread1, thread2, implement run, implement emit
        // might be able to delete struct

char * MyGetter(char *key, int partition_number){
        // works fine but reducers calling too many
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
//                              printf("%s\n", kv[partition_number][r].key);
                                return kv[partition_number][r].value;
}               else{
                        r++;
                        reducecounter[partition_number] = r;
                //       printf("%s\n", kv[partition_number][r-1].key);
                        return kv[partition_number][r-1].value;
}

}
}

void * mapthread(void* arg){
        // assign a file to each map
        threadmap(arg);
        return NULL;
}

int cmpstr(const void *p1, const void *p2){
        return strcmp(* (char * const *) p1, * (char * const *) p2);
}

void * reducethread(void *arg){
        // reduce takes in a key, get_next, and partition number
         pthread_mutex_lock(&lock);
        pnumber++;
         pthread_mutex_unlock(&lock);
        int arg2 = pnumber-1;
        // sort
//      int size = sizeof(keys[arg2])/sizeof(keys[arg2][0]);
        qsort(kv[arg2], pnumbers[arg2], sizeof(kv[arg2][0]), cmpstr);
        // Don't know what to do with getter
        Getter get = malloc(sizeof(void*));
        get = MyGetter;
        int i = 0;
        while(i<pnumbers[arg2]){
                // increment reducecouner down here
                threadreduce(kv[arg2][i].key ,get ,arg2);
//              printf("%s\n", kv[arg2][i].key);
                while(i < pnumbers[arg2] -1  &&
                        strcmp(kv[arg2][i].key, kv[arg2][i+1].key) == 0){
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

        // argv is the list of files that needs to be mapped
        // each file name needs to be passed to the mappers
        pnumber = 0;
        int nprocs = get_nprocs();
        int mapthreadsize;
        int reducethreadsize;
        Numreducers = num_reducers;
        memset(pnumbers, 0, 100 * sizeof(pnumbers[0]));
        memset(reducecounter, 0, 100 * sizeof(reducecounter[0]));
        if(nprocs > num_mappers){
                mapthreadsize = num_mappers;
}       else{
                mapthreadsize = nprocs;
}
        if(nprocs > num_reducers){
                reducethreadsize = num_reducers;
}       else{
                reducethreadsize = nprocs;
}
        pthread_t maparray[mapthreadsize];
        pthread_t reducearray[reducethreadsize];
        int i;
        kv = (KeyValue **) malloc(sizeof(KeyValue *) * 50);
        for(i=0; i<50; i++){
                kv[i] = (KeyValue *) malloc(sizeof(KeyValue) * 500000000);
}

//      for(i=0; i<100; i++){
//              for(j=0; j<1000; j++){
//                      kv[i][j] = (void*)malloc(sizeof(int*) * 4);
//}
//}
        threadmap = map;
        threadreduce = reduce;
        partitioner = partition;
//      char files[100];
//      int filesize;
//      int filesizes[100];
//      int j;
        // sort argv's by file sizes
        // if this gives us problems try without sorting
//      for(i=0; i<argc-1; i++){
//              FILE *fp;
//              fp = fopen(argv[i+1], "w+");
//              fseek(fp, 0, SEEK_END);
//              filesize = ftell(fp);
//              for(j=0; j<i+1; j++){
//                      if(filesizes[j] ==
//}
//}

        // Confused about how argc and number of threads works together
        int k = 0;
        for(i=0; i<argc-1; i++){
                if(k<mapthreadsize){
                        pthread_create(&maparray[k], NULL, mapthread, (void*)argv[i+1]);
                        k++;
}               else{
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

        // Start calling reduce threads
        k = 0;
        for(i=0; i<num_reducers+1; i++){
                if(k<reducethreadsize){
                        // gets a valid hash number from list
                        if(pnumbers[pnumber] != 0){
                        pthread_create(&reducearray[k], NULL, reducethread, NULL);
                        k++;
}                       else{pnumber++;}
                // incrementing pnumber in reducethread
}               else{
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
//      fflush(stdout);
        for(i=0; i<50; i++){
                free(kv[i]);
}
}


void MR_Emit(char *key, char *value){
        // each token is emitted, token can be line in a file
        // This takes in a key and a value
        // Stores them so reducers can use them
//       pthread_mutex_lock(&lock);

                unsigned long hash;
        // don't know num_partitions
        hash = partitioner(key, Numreducers);
       pthread_mutex_lock(&lock);
//              pnumbers[hash]++;
  //     pthread_mutex_unlock(&lock);

        kv[hash][pnumbers[hash]].key = malloc(strlen(key)+1);
        strcpy(kv[hash][pnumbers[hash]].key, key);
//        printf("%s\n", kv[hash][pnumbers[hash]].key);
        kv[hash][pnumbers[hash]].value = malloc(strlen(value) +1);
        strcpy(kv[hash][pnumbers[hash]].value, value);
        kv[hash][pnumbers[hash]].done = 0;
//     printf("%s%lu\n", kv[hash][pnumbers[hash]].key, hash);
        pnumbers[hash]++;
     pthread_mutex_unlock(&lock);
//       pthread_mutex_unlock(&lock);

}


// Given by assignment
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
