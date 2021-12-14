#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include "thread_wrapper.h"


#define MAX_FILES 10

struct file_info{
    int fc; /* number of files */
    unsigned char **next_ptrs; /* next pointer a consumer should read. One per file. 
                                   Consumer grabs one pointer and updates it with an offset */
    size_t *remaining; /* remaining bytes to be read for each file */
    pthread_mutex_t lock; /* grab to take a chunk */ 
};

struct file_info make_fi(int fc, unsigned char *next_ptrs[], size_t remaining[]){
    struct file_info fi;
    fi.fc = fc;
    fi.next_ptrs = next_ptrs; /* next pointer a consumer should read. One per file. 
                                   Consumer grabs one pointer and updates it with an offset */
    fi.remaining = remaining; /* remaining bytes to be read for each file */
    Mutex_init(&fi.lock); /* grab to take a chunk */ 

    return fi;
}

void work(unsigned char* ptr, off_t len){
    printf("Reading from %p, %ld remaining\n", ptr, len);
}

void *thread(void *arg)
{
    struct file_info *fi = (struct file_info *) arg;
    unsigned char *ptr;
    off_t rmng;
    
    Mutex_lock(&fi->lock);
    ptr = fi->next_ptrs[0];
    rmng = fi->remaining[0];
    fi->next_ptrs[0] += 4;
    fi->remaining[0] -= 4;
    Mutex_unlock(&fi->lock); 

    work(ptr, rmng);
    return NULL;
}


int main(int argc, char *argv[])
{
    if(argc <= 1){
		printf("pzip: file1 [file2 ...]\n");
		exit(1);
   	}

    unsigned char *file_ptrs[MAX_FILES]; /* pointers to every mmap'ed file */
    unsigned char *file_ptrs_cpy[MAX_FILES]; /* copy of the initial file ptrs. Will be modified by workers after acquiring a chunk */
    size_t file_sz[MAX_FILES]; /* size of each file */
    size_t file_sz_cpy[MAX_FILES]; /* bytes, of each file, remaining to be read */
    int fc = argc-1; /* number of files passed */

    if(fc>MAX_FILES){
        fc = MAX_FILES;
    }

    for(int i=0; i<fc; i++){
        int fd = open(argv[i+1], O_RDONLY);
        if(fd == -1) perror("open error");
		struct stat s;
		if (fstat(fd,&s) == -1) perror("fstat error");
		file_sz[i] = s.st_size;
        file_sz_cpy[i] = file_sz[i];
        file_ptrs[i] = (unsigned char*) mmap(0, file_sz[i], PROT_READ, MAP_PRIVATE, fd, 0);
        if(file_ptrs[i] == MAP_FAILED) perror("mmap error");
        file_ptrs_cpy[i] = file_ptrs[i];
        close(fd);		
               
    }
    struct file_info fi = make_fi(fc, file_ptrs_cpy, file_sz_cpy);

    pthread_t id[4];
    for(int a = 0; a < 4; a++) Pthread_create(&id[a], 0, thread, (void *) &fi);
    for(int b = 0; b < 4; b++) Pthread_join(id[b], 0);

    for(int i=0; i<fc; i++){
        munmap(file_ptrs[i], file_sz[i]);
    }
    
}
