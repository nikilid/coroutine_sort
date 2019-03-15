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
#include <stdint.h>

typedef struct coroutine{
	uint32_t used;
	uint32_t n;
	uint32_t id;
	uint32_t count;
	time_t time;
	time_t tic; 
	time_t toc; 
	time_t sum_time;
	
	ucontext_t context;	

} coroutine;

static ucontext_t uctx_main;

#define handle_error(msg) \
   do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define stack_size 1024 * 1024

//планировщик


void coroutine_swap(coroutine *uctx_func)
{
	//print32_tf("func%d: started\n", id);
	uctx_func->toc = clock();
	time_t time_work = uctx_func->toc - uctx_func->tic;
	if (time_work >= uctx_func->time || !uctx_func->used){
		uctx_func->sum_time += time_work; 
		int32_t id_next = (1 + uctx_func->id) % uctx_func->n;
		int32_t count = 0;
		while(!((uctx_func+id_next - uctx_func->id)->used)){
			id_next = (id_next + 1)% uctx_func->n ;
			count++;
			if (count == uctx_func->n){
				uctx_func->count += 1;
				if (swapcontext(&(uctx_func->context),  &uctx_main) == -1)
	        		handle_error("swapcontext");
			}
		}
		if (uctx_func->id != id_next){
			uctx_func->count += 1;
		}
		(uctx_func+id_next-uctx_func->id)->tic = clock();
		if (swapcontext(&(uctx_func->context),  &((uctx_func+id_next-uctx_func->id)->context)) == -1)
	    	handle_error("swapcontext");
	} else{
		uctx_func->tic = clock() - time_work;
	}

}
   
//слияние
int32_t* merge(int32_t* left, int32_t* right, int32_t* src, int32_t* dst, int32_t first, int32_t second, coroutine *uctx_func)
{
	coroutine_swap(uctx_func);
	int32_t middle = (first + second) >> 1;
	coroutine_swap(uctx_func);
	int32_t r_count = middle+1, l_count = first;
	coroutine_swap(uctx_func);
	int32_t *target = left == src ? dst : src;
	coroutine_swap(uctx_func);
	for (int32_t i = first; i <= second; i++){
		if(r_count <= second && l_count <= middle){
			coroutine_swap(uctx_func);
			if (left[l_count] > right[r_count]){
				coroutine_swap(uctx_func);
				target[i] = right[r_count++];
			} else {
				coroutine_swap(uctx_func);
				target[i] = left[l_count++];
			}
			coroutine_swap(uctx_func);
		} else {
			coroutine_swap(uctx_func);
			while (r_count <= second){
				coroutine_swap(uctx_func);
				target[i++] = right[r_count++];
				coroutine_swap(uctx_func);	
			}
			while (l_count <= middle){
				coroutine_swap(uctx_func);
				target[i++] = left[l_count++]; 
				coroutine_swap(uctx_func);
			}
		}
	}
	coroutine_swap(uctx_func);
	return target;
}


//сортировка слиянием
int32_t* sort(int32_t* src, int32_t* dst, int32_t first, int32_t second, coroutine *uctx_func )
{
	coroutine_swap(uctx_func);
	if (first == second){
		coroutine_swap(uctx_func);
		return src;
	}
	coroutine_swap(uctx_func);
	if (first + 1 == second){
		coroutine_swap(uctx_func);
		if (src[first] > src[second]){
			coroutine_swap(uctx_func);
			int32_t tmp = src[first];
			coroutine_swap(uctx_func);
			src[first] = src[second];
			coroutine_swap(uctx_func);
			src[second] = tmp;
			coroutine_swap(uctx_func);
			return src;
		} else {
			coroutine_swap(uctx_func);
			return src;
		}
	}
	coroutine_swap(uctx_func);
	int32_t middle = (first + second) >> 1;
	coroutine_swap(uctx_func);
	int32_t *left = sort(src, dst, first, middle, uctx_func);
	coroutine_swap(uctx_func);
	int32_t *right = sort(src, dst, middle+1, second, uctx_func);
	coroutine_swap(uctx_func);
	return merge(left, right, src, dst, first, second, uctx_func);
	
	
} 

//работа отдельной корутины. Чтение файла и его сортировка
static void
my_coroutine(coroutine* uctx_func, char *argv[])
{
	uctx_func->tic = clock();
	int32_t finput;
	FILE* foutput;
	finput = open(argv[uctx_func->id + 1], O_RDONLY);
	coroutine_swap(uctx_func);
	if (finput == -1){
		fprintf(stderr, "Error: Файл %s не открыт\n", argv[uctx_func->id+1] );
		coroutine_swap(uctx_func);
		uctx_func->used=0;
		coroutine_swap(uctx_func);		
		return;
	}
	coroutine_swap(uctx_func);
	size_t mes_size = (size_t) lseek(finput, 0, SEEK_END);
	coroutine_swap(uctx_func);
	if (mes_size == 0){
		fprintf(stderr, "Error: Файл %s пустой\n", argv[uctx_func->id+1] );
		coroutine_swap(uctx_func);
		uctx_func->used=0;
		coroutine_swap(uctx_func);		
		return;
	}
	coroutine_swap(uctx_func);
    lseek(finput, 0, SEEK_SET);
    coroutine_swap(uctx_func);
	char* b = (char*)calloc(mes_size+1, sizeof(char));
	coroutine_swap(uctx_func);
	int32_t i = 0;
	coroutine_swap(uctx_func);
	int32_t* a = (int32_t*)calloc(mes_size, sizeof(int32_t));
	coroutine_swap(uctx_func);
	struct aiocb readrq;
	const struct aiocb *readrqv[1]={&readrq};
	coroutine_swap(uctx_func);
	memset(&readrq, 0, sizeof (readrq));
	coroutine_swap(uctx_func);
   	readrq.aio_fildes = finput;
   	coroutine_swap(uctx_func);
	readrq.aio_buf = b;
	coroutine_swap(uctx_func);
	readrq.aio_nbytes = mes_size;
	coroutine_swap(uctx_func);
	aio_read(&readrq);
	coroutine_swap(uctx_func);
	while(1) {
		aio_suspend(readrqv, 1, NULL);
		coroutine_swap(uctx_func);
		int32_t size =aio_return(&readrq);
		coroutine_swap(uctx_func);
		if (size == mes_size) {
			coroutine_swap(uctx_func);
			break;
		} 
		coroutine_swap(uctx_func);
	}
	coroutine_swap(uctx_func);
	close(finput);
	coroutine_swap(uctx_func);
	b[mes_size] = '\0';
	coroutine_swap(uctx_func);
	char *end;
	int32_t j = 0;
	coroutine_swap(uctx_func);
	char* b1 = b;
	coroutine_swap(uctx_func);
	while(*b1 != '\0'){
		coroutine_swap(uctx_func);
		a[j] = strtol(b1, &end, 10);
		coroutine_swap(uctx_func);
		j++;
		coroutine_swap(uctx_func);
        b1 = end + 1;
        coroutine_swap(uctx_func);
	}
	coroutine_swap(uctx_func);
	free(b);
	coroutine_swap(uctx_func);
	close(finput);
	coroutine_swap(uctx_func);
	int32_t *dst = (int32_t*)calloc(mes_size, sizeof(int32_t));
	int32_t *src;
	coroutine_swap(uctx_func);
	src = sort(a, dst, 0, j-1, uctx_func);
	coroutine_swap(uctx_func);
	foutput = fopen(argv[uctx_func->id+1], "w");
	coroutine_swap(uctx_func);
	for (int32_t i = 0; i < j; i++){
		coroutine_swap(uctx_func);
		fprintf(foutput, "%d ", src[i]);
		coroutine_swap(uctx_func);
	}
	coroutine_swap(uctx_func);
	fclose(foutput);
	coroutine_swap(uctx_func);
	free(a);
	coroutine_swap(uctx_func);
	uctx_func->used = 0;
	coroutine_swap(uctx_func);
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

int32_t num_min_elem(int32_t* a, int32_t n, int32_t* k)
{
	int32_t  j = 0 ;
	while (k[j++] == 1);
	int32_t num_min = j - 1, min = a[j-1]; 
	for (int32_t i = num_min; i < n; i++){
		if (a[i] < min && k[i] != 1){
			num_min = i;
			min = a[i];
		}
	}
	return num_min;
}

void file_merge(FILE* file[], int32_t n, FILE* foutput)
{
	int32_t a[n];
	int32_t count = 0;
	int32_t k[n];
	memset(k,  0,n*sizeof(int32_t));
	for (int32_t i = 0; i < n; i++){
		if( fscanf(file[i], "%d ", &a[i]) == -1){
			count++;
			k[i] = 1;
		}
	}
	while(count != n){
		int32_t num = num_min_elem(a, n, k);
		fprintf(foutput, "%d ", a[num]);
		if( fscanf(file[num], "%d ", &a[num]) == -1){
			count++;
			k[num] = 1;
			fclose(file[num]);
		}
	}


	
}

int
main(int32_t argc, char *argv[])
{
	/* First of all, create a stack for each coroutine. */
	if (argc < 3){
		fprintf(stderr, "Error: не хватает входных данных\n" );		
		return 0;
	}
	uint32_t n = argc - 2;
	time_t time ;
	double a;
	sscanf(argv[argc-1],"%lf", &a);
	a *= CLOCKS_PER_SEC;
	a /= n;
	time = (time_t)a;
	char *func_stack[n];
    coroutine uctx_func[n]; 
    for (int32_t i = 0; i < n; i++){
		func_stack[i] = allocate_stack(STACK_SIG);
		if (getcontext(&uctx_func[i].context) == -1)
			handle_error("getcontext");
		uctx_func[i].used = 1;
		uctx_func[i].n = n;
		uctx_func[i].id = i;
		uctx_func[i].time = time;
		uctx_func[i].count = 0;
		uctx_func[i].sum_time = 0;
		uctx_func[i].context.uc_stack.ss_sp = func_stack[i];
		uctx_func[i].context.uc_stack.ss_size = stack_size;
		uctx_func[i].context.uc_link = &uctx_func[(i+1)%n].context;
		makecontext(&uctx_func[i].context, my_coroutine, 2, &uctx_func[i], argv);
	}
	if (swapcontext(&uctx_main, &uctx_func[0].context) == -1)
		handle_error("swapcontext");
	for (int32_t i = 0; i < n; i++){
		printf("Общее время работы корутины %d: %.3lf миллисекунд; при этом она передавала управление %d раз\n",i, (double)(uctx_func[i].sum_time)*1000/CLOCKS_PER_SEC, uctx_func[i].count);
	}
	time_t tic = clock();
	FILE* file[n];
	FILE* foutput = fopen("a.txt", "w");
	int32_t k = 0;
	for (int32_t j = 0; j < n; j++, k++){
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
