/*Данная программа выполняет сортировку файлов (реализована сортирвка слиянием), которые подаются ей на вход, а затем сливает их в один файл.
Сортировка каждого файла производится в отдельной корутине. Переключение корутин производится через T/N, где T - target latency, N - количество корутин.
Чтение файлов осуществляется асинхронно.
Аргументы командной строки: имена файлов для сортировки, target latency.*/

//#define _XOPEN_SOURCE /* Mac compatibility. */
#include <ucontext.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/mman.h>
#include <string.h>
#include <time.h>
#include <aio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

typedef struct coroutine{
	int used;
	ucontext_t context;	
} coroutine;

static ucontext_t uctx_main;

#define handle_error(msg) \
   do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define stack_size 1024 * 1024

//планировщик

void coroutine_swap(int id, int n, coroutine *uctx_func)
{
	//printf("func%d: started\n", id);
	int id_next = (id + 1)%n;
	int count = 0;
	while(!uctx_func[id_next].used){
		id_next = (id_next + 1)%n;
		count++;
		if (count == n){
			if (swapcontext(&uctx_func[id].context,  &uctx_main) == -1)
	        	handle_error("swapcontext");
		}
	}
	if (swapcontext(&uctx_func[id].context,  &uctx_func[id_next].context) == -1)
	    handle_error("swapcontext");
	//printf("func%d: returning\n", id);

}
   
//слияние
int* merge(int* left, int* right, int* src, int* dst, int first, int second, int id, int n, coroutine *uctx_func, time_t* tic, time_t* toc, time_t time)
{
	int middle = (first + second) >> 1;
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	int r_count = middle+1, l_count = first;
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	int *target = left == src ? dst : src;
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	for (int i = first; i <= second; i++){
		if(r_count <= second && l_count <= middle){
			*toc = clock();
			if (*toc - *tic > time){
				coroutine_swap(id, n, uctx_func);
				*tic = clock();
			}
			if (left[l_count] > right[r_count]){
				target[i] = right[r_count++];
			} else {
				target[i] = left[l_count++];
			}
			*toc = clock();
			if (*toc - *tic > time){
				coroutine_swap(id, n, uctx_func);
				*tic = clock();
			}
		} else {
			*toc = clock();
			if (*toc - *tic > time){
				coroutine_swap(id, n, uctx_func);
				*tic = clock();
			}
			while (r_count <= second){
				target[i++] = right[r_count++];
				*toc = clock();
				if (*toc - *tic > time){
					coroutine_swap(id, n, uctx_func);
					*tic = clock();
				}	
			}
			while (l_count <= middle){
				target[i++] = left[l_count++]; 
				*toc = clock();
				if (*toc - *tic > time){
					coroutine_swap(id, n, uctx_func);
					*tic = clock();
				}
			}
		}
	}
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	return target;

}


//сортировка слиянием
int* sort(int* src, int* dst, int first, int second, int id, int n, coroutine *uctx_func, time_t* tic, time_t* toc, time_t time )
{
	if (first == second){
		return src;
	}
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	if (first + 1 == second){
		*toc = clock();
		if (*toc - *tic > time){
			coroutine_swap(id, n, uctx_func);
			*tic = clock();
		}
		if (src[first] > src[second]){
			*toc = clock();
			if (*toc - *tic > time){
				coroutine_swap(id, n, uctx_func);
				*tic = clock();
			}
			int tmp = src[first];
			src[first] = src[second];
			*toc = clock();
			if (*toc - *tic > time){
				coroutine_swap(id, n, uctx_func);
				*tic = clock();
			}
			src[second] = tmp;
			*toc = clock();
			if (*toc - *tic > time){
				coroutine_swap(id, n, uctx_func);
				*tic = clock();
			}
			return src;
		} else {
			*toc = clock();
			if (*toc - *tic > time){
				coroutine_swap(id, n, uctx_func);
				*tic = clock();
			}
			return src;
		}
	}
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	int middle = (first + second) >> 1;
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	int *left = sort(src, dst, first, middle, id, n, uctx_func, tic, toc, time);
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	int *right = sort(src, dst, middle+1, second, id, n, uctx_func, tic, toc, time);
	*toc = clock();
	if (*toc - *tic > time){
		coroutine_swap(id, n, uctx_func);
		*tic = clock();
	}
	return merge(left, right, src, dst, first, second, id, n, uctx_func, tic, toc, time);
	
	
} 

//работа отдельной корутины. Чтение файла и его сортировка
static void
my_coroutine(int id, int n, coroutine *uctx_func, char*argv[], time_t time)
{
	clock_t tic = clock();
	clock_t toc;
	int finput;
	FILE* foutput;
	finput = open(argv[id+1], O_RDONLY);
	if (finput == -1){
		uctx_func[id].used=0;
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}		
		return;
	}
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	size_t mes_size = (size_t) lseek(finput, 0, SEEK_END);
	if (mes_size == 0){
		uctx_func[id].used=0;
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}		
		return;
	}
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
    lseek(finput, 0, SEEK_SET);
    toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	char* b = (char*)calloc(mes_size+1, sizeof(char));
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	int i = 0;
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	int* a = (int*)calloc(mes_size, sizeof(int));
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	struct aiocb readrq;
	const struct aiocb *readrqv[1]={&readrq};
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	memset(&readrq, 0, sizeof (readrq));
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
   	readrq.aio_fildes = finput;
   	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	readrq.aio_buf = b;
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	readrq.aio_nbytes = mes_size;
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	aio_read(&readrq);
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	int sum = 0;
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	while(1) {
		aio_suspend(readrqv, 1, NULL);
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}
		int size =aio_return(&readrq);
		//sum+=size;
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}
		//printf("%s\n",argv[id+1]);
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}
		if (size == mes_size) {
			break;
		} 
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}
	}
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	close(finput);
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	b[mes_size] = '\0';
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	//printf("%s\n",b );
	char *end;
	int j = 0;
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	char* b1 = b;
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	while(*b1 != '\0'){
		a[j] = strtol(b1, &end, 10);
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}
		j++;
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}
        b1 = end + 1;
        toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}
	}
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	free(b);
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	close(finput);
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	int *dst = (int*)calloc(mes_size, sizeof(int));
	int *src;
	src = sort(a, dst, 0, j-1, id, n, uctx_func, &tic, &toc, time);
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	foutput = fopen(argv[id+1], "w");
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	for (int i = 0; i < j; i++){
		fprintf(foutput, "%d ", src[i]);
		toc = clock();
		if (toc - tic > time){
			coroutine_swap(id, n, uctx_func);
			tic = clock();
		}
	}
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	fclose(foutput);
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	free(a);
	toc = clock();
	if (toc - tic > time){
		coroutine_swap(id, n, uctx_func);
		tic = clock();
	}
	uctx_func[id].used=0;
	coroutine_swap(id, n, uctx_func);
}


/**
 * Below you can see 3 different ways of how to allocate stack.
 * You can choose any. All of them do in fact the same.
 */

static void *
allocate_stack_sig()
{
	void *stack = malloc(stack_size);
	stack_t ss;
	ss.ss_sp = stack;
	ss.ss_size = stack_size;
	ss.ss_flags = 0;
	sigaltstack(&ss, NULL);
	return stack;
}

static void *
allocate_stack_mmap()
{
	return mmap(NULL, stack_size, PROT_READ | PROT_WRITE | PROT_EXEC,
		    MAP_ANON | MAP_PRIVATE, -1, 0);
}

static void *
allocate_stack_mprot()
{
	void *stack = malloc(stack_size);
	mprotect(stack, stack_size, PROT_READ | PROT_WRITE | PROT_EXEC);
	return stack;
}

enum stack_type {
	STACK_MMAP,
	STACK_SIG,
	STACK_MPROT
};

/**
 * Use this wrapper to choose your favourite way of stack
 * allocation.
 */
static void *
allocate_stack(enum stack_type t)
{
	switch(t) {
	case STACK_MMAP:
		return allocate_stack_mmap();
	case STACK_SIG:
		return allocate_stack_sig();
	case STACK_MPROT:
		return allocate_stack_mprot();
	}
}

int num_min_elem(int* a, int n, int* k)
{
	int  j = 0 ;
	while (k[j++] == 1);
	int num_min = j - 1, min = a[j-1]; 
	for (int i = num_min; i < n; i++){
		if (a[i] < min && k[i] != 1){
			num_min = i;
			min = a[i];
		}
	}
	return num_min;
}

void file_merge(FILE* file[], int n, FILE* foutput)
{
	int a[n];
	int count = 0;
	int k[n];
	memset(k,  0,n*sizeof(int));
	for (int i = 0; i < n; i++){
		if( fscanf(file[i], "%d ", &a[i]) == -1){
			count++;
			k[i] = 1;
		}
	}
	while(count != n){
		int num = num_min_elem(a, n, k);
		fprintf(foutput, "%d ", a[num]);
		if( fscanf(file[num], "%d ", &a[num]) == -1){
			count++;
			k[num] = 1;
			fclose(file[num]);
		}
	}


	
}

int
main(int argc, char *argv[])
{
	/* First of all, create a stack for each coroutine. */
	int n = argc - 2;
	time_t time ;
	sscanf(argv[argc-2],"%lf", (double*)&time);
	time /= n;
	char *func_stack[n];
    coroutine uctx_func[n]; 
    for (int i = 0; i < n; i++){
		func_stack[i] = allocate_stack(STACK_SIG);
		if (getcontext(&uctx_func[i].context) == -1)
			handle_error("getcontext");
		uctx_func[i].used = 1;
		uctx_func[i].context.uc_stack.ss_sp = func_stack[i];
		uctx_func[i].context.uc_stack.ss_size = stack_size;
		uctx_func[i].context.uc_link = &uctx_func[(i+1)%n].context;
		makecontext(&uctx_func[i].context, my_coroutine, 5, i, n, uctx_func, argv, time);
	}

	if (swapcontext(&uctx_main, &uctx_func[0].context) == -1)
		handle_error("swapcontext");

	FILE* file[n];
	FILE* foutput = fopen("a.txt", "w");
	int k = 0;
	for (int j = 0; j < n; j++, k++){
		file[j] = fopen(argv[j+1], "r");
		if (file[k] == NULL){
			//fprintf(stderr, "Error: Файл не открыт\n" );
			k--;
		}
	}
	if (k != n){
		n = k;
	}
	file_merge(file, n, foutput);
	printf("main: exiting\n");
	return 0;
}
