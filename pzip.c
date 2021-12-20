#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include "thread_wrapper.h"


#define MAX_FILES 10
#define CHUNK_SIZE 128 
#define min(x,y) ((x)<(y) ? x : y)

typedef struct cmpdata{
    int n;
    char c;
    struct cmpdata *next;
} cmpdata_t;

struct sh_buffer{
    int idx; /* index of the next chunk that has to be pushed to the global buffer. Each chunk may contain 1 or more cmpdata_t */
    cmpdata_t *last_node; /*when a list is sent to stdout, all the nodes are sent, except the last one. last_node points to that node */
    pthread_cond_t my_turn; /* thread waits if it's not his turn. After pushing to buffer, wake all sleeping threads
			       so they can check if it's their turn, or otherwise go to sleep.  */
    pthread_mutex_t lock; /* grab to update index. Update once the current chunk has been pushed */ 
};

struct file_info{
    int fc; /* number of files */
    int fidx; /* index of the file to be read. If remaining[fidx] == 0 fidx++ */
    int chunk_idx; /* every thread takes one */
    unsigned char **next_ptrs; /* next pointer a consumer should read. One per file. 
                                   Consumer grabs one pointer and updates it with an offset */
    size_t *remaining; /* remaining bytes to be read for each file */
    struct sh_buffer *shbuff;
    pthread_mutex_t lock; /* grab to take a chunk */ 
};

cmpdata_t * make_cmpdata(int n, char c){
    cmpdata_t *tmp = malloc(sizeof(cmpdata_t));
    if(tmp == NULL) exit(1);
    tmp->n = n;
    tmp->c = c;
    tmp->next = NULL;
    return tmp;    
}

struct sh_buffer make_shbuff(){
	struct sh_buffer shbuff;
	shbuff.idx = 0;
    shbuff.last_node = NULL;
	Cond_init(&shbuff.my_turn);
	Mutex_init(&shbuff.lock);
	return shbuff;
}

struct file_info make_fi(int fc, unsigned char *next_ptrs[], size_t remaining[], struct sh_buffer *shbuff){
    struct file_info fi;
    fi.fc = fc;
    fi.fidx = 0;
    fi.next_ptrs = next_ptrs; /* next pointer a consumer should read. One per file. 
                                   Consumer grabs one pointer and updates it with an offset */
    fi.chunk_idx = 0;
    fi.shbuff = shbuff;
    fi.remaining = remaining; /* remaining bytes to be read for each file */
    Mutex_init(&fi.lock); /* grab to take a chunk */ 

    return fi;
}

/* returns head of the private linked list */
cmpdata_t *work(unsigned char* ptr, off_t len){
    assert(len > 0);
    cmpdata_t *priv_ls = NULL;  /* head of private list */
    cmpdata_t *tail = NULL;  
    int n = 1;
    char c = *ptr; 
    ptr++;
    len--;
    while(len >= 0){
       if (*ptr != c || len == 0){
           cmpdata_t *node = make_cmpdata(n,c);
           if(priv_ls == NULL){
               priv_ls = node;
               tail = node;
           }else{
               tail->next = node;
               tail = node;
           }
           c = *ptr;
           n = 1;
       }else{
           n++;
       }

       ptr++;
       len--; 
    }

    return priv_ls;
    //printf("Reading from %p, %ld remaining\n", ptr, len);
}

void node_to_stdout(int n, char c){
    fwrite(&n, sizeof(int),1,stdout);
    putchar(c);
    //printf("%d%c\n",n,c);
}

/* if reading last private list corresponding to the last chunk of all the files, 
 * then send to stdout last node as well; otherwise leave it for next thread  */

int lookup_cond(cmpdata_t *pv_ls, off_t rmng, int fidx, int fc){ 
    if(fidx == fc-1 && rmng <= CHUNK_SIZE){
        return pv_ls != NULL;
    }else{
        return pv_ls->next != NULL;
    }
}

void lookup_ls(cmpdata_t **prev_node, cmpdata_t *pv_ls, off_t rmng, int fidx, int fc){
    assert(pv_ls != NULL); 
    
    if(*prev_node != NULL && ((*prev_node)->c == pv_ls->c)){
        pv_ls->n += (*prev_node)->n;
    }else if(*prev_node != NULL && ((*prev_node)->c != pv_ls->c)){
        node_to_stdout((*prev_node)->n,(*prev_node)->c); 
    }

    free(*prev_node);
    cmpdata_t *next = NULL;
     
    while(lookup_cond(pv_ls, rmng, fidx, fc)){
        next = pv_ls->next;
        node_to_stdout(pv_ls->n, pv_ls->c);  
        free(pv_ls);
        pv_ls = next;
    }
    *prev_node = pv_ls;
}

void push_to_buff(struct sh_buffer *shbuff, int idx, cmpdata_t *pv_ls, off_t len, off_t remaining, int fidx, int fc){ 
    Mutex_lock(&shbuff->lock);
    while(idx != shbuff->idx){
        Cond_wait(&shbuff->my_turn, &shbuff->lock); 
    }
    
    //printf("I'm pushing chunk %d, %ld bytes read\n", idx, len);
    lookup_ls(&shbuff->last_node, pv_ls, remaining, fidx, fc);
    shbuff->idx++;
    Cond_broadcast(&shbuff->my_turn);
    Mutex_unlock(&shbuff->lock);

}
void *thread(void *arg)
{
    struct file_info *fi = (struct file_info *) arg;
    int idx;
    unsigned char *ptr;
    off_t rmng;
    int fidx; /* file index */
   
    for(;;){
        Mutex_lock(&fi->lock);
        fidx = fi->fidx;
        ptr = fi->next_ptrs[fidx];
        idx = fi->chunk_idx;
        fi->chunk_idx++;
        rmng = fi->remaining[fidx];
        off_t offset = min(rmng, CHUNK_SIZE); 
        fi->next_ptrs[fidx] += offset;
        fi->remaining[fidx] -= offset;
        if(fi->remaining[fidx] == 0) fi->fidx++;
        Mutex_unlock(&fi->lock);
        if(rmng==0){
           if(fidx < (fi->fc - 1)){
               continue;
           }else{
               break; 
           }
        }
        cmpdata_t * data = work(ptr, offset);
        push_to_buff(fi->shbuff, idx, data, offset, rmng, fidx, fi->fc);
    } 
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

    struct sh_buffer shbuff = make_shbuff();
    struct file_info fi = make_fi(fc, file_ptrs_cpy, file_sz_cpy, &shbuff);

    pthread_t id[4];
    for(int a = 0; a < 4; a++) Pthread_create(&id[a], 0, thread, (void *) &fi);
    for(int b = 0; b < 4; b++) Pthread_join(id[b], 0);

    for(int i=0; i<fc; i++){
        munmap(file_ptrs[i], file_sz[i]);
    }
    
    
}
