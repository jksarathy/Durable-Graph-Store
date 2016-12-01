
#ifndef HEADER_FILE
#define HEADER_FILE

#define _USE_MISC
//#define _GNU_SOURCE
//#define _BSD_SOURCE

#define SUCCESS 200
#define EXISTS 204
#define ERROR 400 
#define NO_SPACE 507 

#define SB_SIZE 20
#define LB_SIZE 4096
#define LB_HEADER_SIZE 16
#define MAX_LOG_ENTRIES 204
#define LOG_ENTRY_SIZE 20
#define LOG_START 20
#define CHECKPOINT_SIZE 1024 * 1024 * 1024 * 4
#define CHECKSUM_CONST 5

#define ADD_NODE 0
#define ADD_EDGE 1
#define REMOVE_NODE 2
#define REMOVE_EDGE 3

#include "Graph.h"
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>


typedef struct {
  uint32_t gen_num;
  uint64_t checksum;
  uint32_t log_start;
  uint32_t log_size;
} Superblock;

typedef struct {
  uint32_t gen_num;
  uint32_t num_entries;
  uint64_t checksum;	
} Logblock;

typedef struct {
	uint32_t opcode;
	uint64_t node_a_id;
	uint64_t node_b_id;
} LogEntry;

typedef struct {
  uint64_t node_a_id;
  uint64_t node_b_id;
} Edge;

typedef struct {
  uint64_t num_nodes;
  uint64_t num_edges;
} CPGraph;

typedef struct {
  int fd;
  Graph *g;
  uint32_t gen_num;
  uint32_t logtail;
} Data;

Data *init_data(int fd, int fflag);
uint64_t compute_sb_checksum(Superblock *sb);
uint64_t compute_lb_checksum(Logblock *lb);
int valid_sb_checksum(int fd);
int valid_lb_checksum(int fd);
void init_sb(int fd);
void set_sb_gen(int fd, uint32_t gen_num);
uint32_t get_sb_gen(int fd);
int log_full(uint32_t logtail);
void add_log(Data *d, uint32_t opcode, uint64_t node_a_id, uint64_t node_b_id);
int write_checkpoint(Data *d);
void read_checkpoint(int fd, Graph *g);
void clear_checkpoint(int fd);
uint32_t play_log(int fd, uint32_t gen_num, Graph *g);

#endif
