/*Данная программа выполняет сортировку файлов (реализована сортирвка слиянием), которые подаются ей на вход, а затем сливает их в один файл.
Сортировка каждого файла производится в отдельной корутине. Переключение корутин производится через T/N, где T - target latency, N - количество корутин.
Чтение файлов осуществляется асинхронно.
Аргументы командной строки: имена файлов для сортировки, target latency.*/

//#define _XOPEN_SOURCE /* Mac compatibility. */
#include <ucontext.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>
#include <time.h>
#include <aio.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

typedef struct coroutine{
	int used;
	time_t tic; 
	time_t toc; 
	time_t sum_time;
	int count;
	ucontext_t context;	

} coroutine;

static ucontext_t uctx_main;

#define handle_error(msg) \
   do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define stack_size 1024 * 1024

//планировщик


void coroutine_swap(int id, int n, coroutine *uctx_func, time_t time)
{
	//printf("func%d: started\n", id);
	uctx_func[id].toc = clock();
	time_t time_work = uctx_func[id].toc - uctx_func[id].tic;
	if (time_work >= time || !uctx_func[id].used){
		uctx_func[id].sum_time += time_work; 
		int id_next = (id + 1)%n;
		int count = 0;
		while(!uctx_func[id_next].used){
			id_next = (id_next + 1)%n;
			count++;
			if (count == n){
				uctx_func[id].count += 1;
				for (int i = 0; i < n; i++){
					printf("Общее время работы корутины %d: %.3lf миллисекунд; при этом она передавала управление %d раз\n",i, (double)(uctx_func[i].sum_time)*1000/CLOCKS_PER_SEC, uctx_func[i].count);
				}
				if (swapcontext(&uctx_func[id].context,  &uctx_main) == -1)
	        		handle_error("swapcontext");
			}
		}
		if (id != id_next){
			uctx_func[id].count += 1;
		}
		uctx_func[id_next].tic = clock();
		if (swapcontext(&uctx_func[id].context,  &uctx_func[id_next].context) == -1)
	    	handle_error("swapcontext");
	} else{
		uctx_func[id].tic = clock() - time_work;
	}

}
   
//слияние
int* merge(int* left, int* right, int* src, int* dst, int first, int second, int id, int n, coroutine *uctx_func, time_t time)
{
	coroutine_swap(id, n, uctx_func, time);
	int middle = (first + second) >> 1;
	coroutine_swap(id, n, uctx_func, time);
	int r_count = middle+1, l_count = first;
	coroutine_swap(id, n, uctx_func, time);
	int *target = left == src ? dst : src;
	coroutine_swap(id, n, uctx_func, time);
	for (int i = first; i <= second; i++){
		if(r_count <= second && l_count <= middle){
			coroutine_swap(id, n, uctx_func, time);
			if (left[l_count] > right[r_count]){
				coroutine_swap(id, n, uctx_func, time);
				target[i] = right[r_count++];
			} else {
				coroutine_swap(id, n, uctx_func, time);
				target[i] = left[l_count++];
			}
			coroutine_swap(id, n, uctx_func, time);
		} else {
			coroutine_swap(id, n, uctx_func, time);
			while (r_count <= second){
				coroutine_swap(id, n, uctx_func, time);
				target[i++] = right[r_count++];
				coroutine_swap(id, n, uctx_func, time);	
			}
			while (l_count <= middle){
				coroutine_swap(id, n, uctx_func, time);
				target[i++] = left[l_count++]; 
				coroutine_swap(id, n, uctx_func, time);
			}
		}
	}
	coroutine_swap(id, n, uctx_func, time);
	return target;
}


//сортировка слиянием
int* sort(int* src, int* dst, int first, int second, int id, int n, coroutine *uctx_func, time_t time )
{
	coroutine_swap(id, n, uctx_func, time);
	if (first == second){
		coroutine_swap(id, n, uctx_func, time);
		return src;
	}
	coroutine_swap(id, n, uctx_func, time);
	if (first + 1 == second){
		coroutine_swap(id, n, uctx_func, time);
		if (src[first] > src[second]){
			coroutine_swap(id, n, uctx_func, time);
			int tmp = src[first];
			coroutine_swap(id, n, uctx_func, time);
			src[first] = src[second];
			coroutine_swap(id, n, uctx_func, time);
			src[second] = tmp;
			coroutine_swap(id, n, uctx_func, time);
			return src;
		} else {
			coroutine_swap(id, n, uctx_func, time);
			return src;
		}
	}
	coroutine_swap(id, n, uctx_func, time);
	int middle = (first + second) >> 1;
	coroutine_swap(id, n, uctx_func, time);
	int *left = sort(src, dst, first, middle, id, n, uctx_func, time);
	coroutine_swap(id, n, uctx_func, time);
	int *right = sort(src, dst, middle+1, second, id, n, uctx_func, time);
	coroutine_swap(id, n, uctx_func, time);
	return merge(left, right, src, dst, first, second, id, n, uctx_func, time);
	
	
} 

//работа отдельной корутины. Чтение файла и его сортировка
static void
my_coroutine(int id, int n, coroutine *uctx_func, char*argv[], time_t time)
{
	uctx_func[id].tic = clock();
	int finput;
	FILE* foutput;
	finput = open(argv[id+1], O_RDONLY);
	coroutine_swap(id, n, uctx_func, time);
	if (finput == -1){
		fprintf(stderr, "Error: Файл %s не открыт\n", argv[id+1] );
		coroutine_swap(id, n, uctx_func, time);
		uctx_func[id].used=0;
		coroutine_swap(id, n, uctx_func, time);		
		return;
	}
	coroutine_swap(id, n, uctx_func, time);
	size_t mes_size = (size_t) lseek(finput, 0, SEEK_END);
	coroutine_swap(id, n, uctx_func, time);
	if (mes_size == 0){
		fprintf(stderr, "Error: Файл %s пустой\n", argv[id+1] );
		coroutine_swap(id, n, uctx_func, time);
		uctx_func[id].used=0;
		coroutine_swap(id, n, uctx_func, time);		
		return;
	}
	coroutine_swap(id, n, uctx_func, time);
    lseek(finput, 0, SEEK_SET);
    coroutine_swap(id, n, uctx_func, time);
	char* b = (char*)calloc(mes_size+1, sizeof(char));
	coroutine_swap(id, n, uctx_func, time);
	int i = 0;
	coroutine_swap(id, n, uctx_func, time);
	int* a = (int*)calloc(mes_size, sizeof(int));
	coroutine_swap(id, n, uctx_func, time);
	struct aiocb readrq;
	const struct aiocb *readrqv[1]={&readrq};
	coroutine_swap(id, n, uctx_func, time);
	memset(&readrq, 0, sizeof (readrq));
	coroutine_swap(id, n, uctx_func, time);
   	readrq.aio_fildes = finput;
   	coroutine_swap(id, n, uctx_func, time);
	readrq.aio_buf = b;
	coroutine_swap(id, n, uctx_func, time);
	readrq.aio_nbytes = mes_size;
	coroutine_swap(id, n, uctx_func, time);
	aio_read(&readrq);
	coroutine_swap(id, n, uctx_func, time);
	while(1) {
		aio_suspend(readrqv, 1, NULL);
		coroutine_swap(id, n, uctx_func, time);
		int size =aio_return(&readrq);
		coroutine_swap(id, n, uctx_func, time);
		if (size == mes_size) {
			coroutine_swap(id, n, uctx_func, time);
			break;
		} 
		coroutine_swap(id, n, uctx_func, time);
	}
	coroutine_swap(id, n, uctx_func, time);
	close(finput);
	coroutine_swap(id, n, uctx_func, time);
	b[mes_size] = '\0';
	coroutine_swap(id, n, uctx_func, time);
	char *end;
	int j = 0;
	coroutine_swap(id, n, uctx_func, time);
	char* b1 = b;
	coroutine_swap(id, n, uctx_func, time);
	while(*b1 != '\0'){
		coroutine_swap(id, n, uctx_func, time);
		a[j] = strtol(b1, &end, 10);
		coroutine_swap(id, n, uctx_func, time);
		j++;
		coroutine_swap(id, n, uctx_func, time);
        b1 = end + 1;
        coroutine_swap(id, n, uctx_func, time);
	}
	coroutine_swap(id, n, uctx_func, time);
	free(b);
	coroutine_swap(id, n, uctx_func, time);
	close(finput);
	coroutine_swap(id, n, uctx_func, time);
	int *dst = (int*)calloc(mes_size, sizeof(int));
	int *src;
	coroutine_swap(id, n, uctx_func, time);
	src = sort(a, dst, 0, j-1, id, n, uctx_func, time);
	coroutine_swap(id, n, uctx_func, time);
	foutput = fopen(argv[id+1], "w");
	coroutine_swap(id, n, uctx_func, time);
	for (int i = 0; i < j; i++){
		coroutine_swap(id, n, uctx_func, time);
		fprintf(foutput, "%d ", src[i]);
		coroutine_swap(id, n, uctx_func, time);
	}
	coroutine_swap(id, n, uctx_func, time);
	fclose(foutput);
	coroutine_swap(id, n, uctx_func, time);
	free(a);
	coroutine_swap(id, n, uctx_func, time);
	uctx_func[id].used = 0;
	coroutine_swap(id, n, uctx_func, time);
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
	if (argc < 3){
		fprintf(stderr, "Error: не хватает входных данных\n" );		
		return 0;
	}
	int n = argc - 2;
	int time ;
	sscanf(argv[argc-1],"%d", &time);
	time /= n;
	char *func_stack[n];
    coroutine uctx_func[n]; 
    for (int i = 0; i < n; i++){
		func_stack[i] = allocate_stack(STACK_SIG);
		if (getcontext(&uctx_func[i].context) == -1)
			handle_error("getcontext");
		uctx_func[i].used = 1;
		uctx_func[i].sum_time = 0;
		uctx_func[i].count = 0;
		uctx_func[i].context.uc_stack.ss_sp = func_stack[i];
		uctx_func[i].context.uc_stack.ss_size = stack_size;
		uctx_func[i].context.uc_link = &uctx_func[(i+1)%n].context;
		makecontext(&uctx_func[i].context, my_coroutine, 5, i, n, uctx_func, argv, time);
	}

	if (swapcontext(&uctx_main, &uctx_func[0].context) == -1)
		handle_error("swapcontext");
	time_t tic = clock();
	FILE* file[n];
	FILE* foutput = fopen("a.txt", "w");
	int k = 0;
	for (int j = 0; j < n; j++, k++){
		file[j] = fopen(argv[j+1], "r");
		if (file[k] == NULL){
			k--;
		}
	}
	if (k != n){
		n = k;
		if (n == 0){
			fprintf(stderr, "Error: нет валидных файлов\n" );
			return 0;
		}
	}
	file_merge(file, n, foutput);
	time_t toc = clock();
	printf("Время выполнения слияния файлов: %.3lf миллисекунд\n", (double)(toc-tic)*1000/CLOCKS_PER_SEC);
	printf("Время работы программы: %.3lf миллисекунд\n", (double)clock()*1000/CLOCKS_PER_SEC);
	return 0;
}
