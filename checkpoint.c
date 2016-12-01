#include "checkpoint.h"

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#define handle_error(msg) \
  do { perror(msg); exit(EXIT_FAILURE); } while (0)

uint32_t LOG_SIZE = 1024*1024*1024;

Data *init_data(int fd, int fflag) {
  Data *data = (Data *) malloc(sizeof(Data));
  Graph* graph = new Graph();

  data->g = graph;
  data->fd = fd;
  data->gen_num = 0;
  data->logtail = 0;

  uint32_t gen_num;

  if (fflag) {
    clear_checkpoint(fd);
    if (valid_sb_checksum(fd)) {
      printf("Init data: -f and valid checksum\n");
      gen_num = get_sb_gen(fd) + 1;
      printf("Init data: gen num %d\n", gen_num);
      init_sb(fd);
      set_sb_gen(fd, gen_num);
    } else {
      printf("Init data: -f and invalid checksum\n");
      gen_num = 0;
      init_sb(fd);
    }
    data->g = graph;
    data->gen_num = gen_num;
    data->logtail = 0;
   
  } else {
    if (valid_sb_checksum(fd)) {
      printf("Init data: valid checksum\n");
      uint32_t logtail;
      gen_num = get_sb_gen(fd);
      printf("Init data: sb_gen is %d\n", gen_num);
      read_checkpoint(fd, graph);
      logtail = play_log(fd, gen_num, graph);

      data->g = graph;
      data->gen_num = gen_num;
      data->logtail = logtail;
    } else {
      handle_error("superblock invalid\n");
    }
  }

  return data;
}

uint64_t compute_sb_checksum(Superblock *sb) {
  //return (((uint64_t) sb->gen_num)^((uint64_t) sb->log_start)^((uint64_t) sb->log_size)) + (uint64_t) CHECKSUM_CONST;
  //uint64_t ret_val = sb->gen_num + CHECKSUM_CONST;
  uint64_t ret_val = (uint64_t) CHECKSUM_CONST;
  return ret_val;
}

uint64_t compute_lb_checksum(Logblock *lb) {
  //return (((uint64_t) lb->gen_num)^((uint64_t) lb->num_entries)) + (uint64_t)CHECKSUM_CONST;
  uint64_t ret_val = (uint64_t) CHECKSUM_CONST;
  return ret_val;
}

int valid_sb_checksum(int fd) {
  uint32_t offset;
  uint32_t bytes_read;
  Superblock *sb;

  offset = lseek(fd, 0, SEEK_SET);
  if (offset != 0) {
    handle_error("could not find superblock");
  }

  sb = (Superblock *) mmap(NULL, SB_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (sb == MAP_FAILED) {
    handle_error("could not mmap superblock");
  }

  bytes_read = read(fd, sb, SB_SIZE);
  if (bytes_read != SB_SIZE) {
    handle_error("could not read superblock");
  }

  printf("Valid sb checksum: gen-num: %d, checksum: %llu, logstart: %d, logsize %llu\n",
          sb->gen_num, sb->checksum, sb->log_start, sb->log_size);
  if (sb->checksum == compute_sb_checksum(sb)) {
    return TRUE;
  }
  else {
    return FALSE;
  }
}


int valid_lb_checksum(int fd, uint32_t logtail) {
  uint32_t offset;
  uint32_t cs;
  uint32_t bytes_read;
  Logblock *lb;

  offset = LOG_START + LB_SIZE * logtail;
  // Lseek with offset to find latest block header
  if (lseek(fd, offset, SEEK_SET) != offset) {
    handle_error("could not find log tail\n");
  }

  lb = (Logblock *) mmap(NULL, LB_HEADER_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (lb == MAP_FAILED) {
    handle_error("could not mmap logblock header");
  }

  bytes_read = read(fd, lb, LB_HEADER_SIZE);
  if (bytes_read != LB_HEADER_SIZE) {
    handle_error("could not read superblock");
  }

  cs = lb->checksum;
  if (cs == compute_lb_checksum(lb)) {
    return TRUE;
  }
  else {
    return FALSE;
  }
}

void init_sb(int fd) {
  uint32_t offset;
  uint32_t bytes_wrote;
  Superblock *sb;

  offset = lseek(fd, 0, SEEK_SET);
  if (offset != 0) {
    handle_error("could not find superblock");
  }

  sb = (Superblock *) mmap(NULL, SB_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (sb == MAP_FAILED) {
    handle_error("could not mmap superblock");
  }

  sb->gen_num = 0;
  sb->log_start = LOG_START;
  sb->log_size = LOG_SIZE;
  sb->checksum = compute_sb_checksum(sb);

  printf("Init sb: gen-num: %d, checksum: %llu, logstart: %d, logsize %d\n",
          sb->gen_num, sb->checksum, sb->log_start, sb->log_size);

  bytes_wrote = write(fd, sb, SB_SIZE);
  if (bytes_wrote != SB_SIZE) {
    handle_error("could not write superblock");
  }
}

void set_sb_gen(int fd, uint32_t gen_num) {
  uint32_t offset;
  uint32_t bytes_read;
  uint32_t bytes_wrote;
  Superblock *sb;

  offset = lseek(fd, 0, SEEK_SET);
  if (offset != 0) {
    handle_error("could not find superblock\n");
  }

  sb = (Superblock *) mmap(NULL, SB_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (sb == MAP_FAILED) {
    handle_error("could not mmap superblock\n");
  }

  bytes_read = read(fd, sb, SB_SIZE);
  if (bytes_read != SB_SIZE) {
    handle_error("could not read superblock\n");
  }

  sb->gen_num = gen_num;
  sb->checksum = compute_sb_checksum(sb);

  offset = lseek(fd, 0, SEEK_SET);
  if (offset != 0) {
    handle_error("could not find superblock\n");
  }
  bytes_wrote = write(fd, sb, SB_SIZE);
  if (bytes_wrote != SB_SIZE) {
    handle_error("could not write superblock\n");
  }
}

uint32_t get_sb_gen(int fd) {
  uint32_t offset;
  uint32_t gen_num;
  uint32_t bytes_read;
  Superblock *sb;

  offset = lseek(fd, 0, SEEK_SET);
  if (offset != 0) {
    handle_error("could not find superblock\n");
  }

  sb = (Superblock *) mmap(NULL, SB_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (sb == MAP_FAILED) {
    handle_error("could not mmap superblock\n");
  }

  bytes_read = read(fd, sb, SB_SIZE);
  if (bytes_read != SB_SIZE) {
    handle_error("could not read superblock\n");
  }

  gen_num = sb->gen_num;

  return gen_num;
}

int log_full(uint32_t logtail) {
  printf("Log_full:\n");
  if (logtail >= LOG_SIZE) {
    printf("Log_full: logtail %d\n", logtail);
    return TRUE;
  }
  else {
    printf("Log_full: logtail %d\n", logtail);
    return FALSE;
  }
}

void add_log(Data *d, uint32_t opcode, uint64_t node_a_id, uint64_t node_b_id) {

  int fd = d->fd;

  printf("Add log: log tail %d\n", d->logtail);
  uint32_t offset = LOG_START + LB_SIZE * (d->logtail);

  if (lseek(fd, offset, SEEK_SET) != offset) {
    handle_error("could not find log tail\n");
  }
  // Mmap block header and cast to Logblock
  Logblock *logblock = (Logblock *) mmap(NULL, LB_HEADER_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (logblock == MAP_FAILED) {
    handle_error("could not mmap superblock\n");
  }
  // Read header 
  uint32_t bytes_read = read(fd, logblock, LB_HEADER_SIZE);
  if (bytes_read != LB_HEADER_SIZE) {
    handle_error("could not read logblock header\n");
  }
  // If checksum invalid, initialize header as new logblock
  if (logblock->checksum != compute_lb_checksum(logblock) || logblock->gen_num != d->gen_num) {
    printf("Add log: new block\n");
    logblock->gen_num = d->gen_num;
    logblock->num_entries = 0;
    logblock->checksum = compute_lb_checksum(logblock);
  }

  printf("Add log: logblock entries %d\n", logblock->num_entries);
  uint32_t entry_offset = LOG_START + (LB_SIZE * (d->logtail)) + 
                          LB_HEADER_SIZE + (LOG_ENTRY_SIZE * (logblock->num_entries));
  if (lseek(fd, entry_offset, SEEK_SET) != entry_offset) {
    handle_error("could not find log entry\n");
  }
  // Mmap entry and cast to entry struct
  LogEntry *new_entry = (LogEntry *) mmap(NULL, LOG_ENTRY_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (new_entry == MAP_FAILED) {
    handle_error("could not mmap new entry\n");
  }
  // Fill out info in entry struct
  new_entry->opcode = opcode;
  new_entry->node_a_id = node_a_id;
  new_entry->node_b_id = node_b_id;
  printf("Add log: Created entry with opcode %d, node_a_id %llu, node_b_id %llu\n", 
          new_entry->opcode, new_entry->node_a_id, new_entry->node_b_id);

  // Write entry struct 
  uint32_t bytes_wrote = write(fd, new_entry, LOG_ENTRY_SIZE);
  if (bytes_wrote != LOG_ENTRY_SIZE) {
    handle_error("could not write new entry\n");
  }

  // Increment header num_entries
  logblock->num_entries += 1;
  logblock->checksum = compute_lb_checksum(logblock);

  // Lseek to beginning of latest block
  if (lseek(fd, offset, SEEK_SET) != offset) {
    handle_error("could not find log tail\n");
  }
  // Write header
  bytes_wrote = write(fd, logblock, LB_HEADER_SIZE);
  if (bytes_wrote != LB_HEADER_SIZE) {
    handle_error("could not write log header\n");
  }
  // If num entries was max, increment log tail
  if (logblock->num_entries == MAX_LOG_ENTRIES) {
    d->logtail += 1;
  }

}

int write_checkpoint(Data *d) {
  printf("Write checkpoint:\n");
  int fd = d->fd;

  // Get info from graph
  Graph *g = d->g;
  uint64_t num_nodes = g->numNodes();
  uint64_t num_edges = g->numEdges();

  printf("Write checkpoint:got header info\n");

  std::vector<uint64_t> nodes = g->getNodes();
  std::vector<std::pair<uint64_t, uint64_t> > edges = g->getEdges();

  printf("Write checkpoint:got graph info\n");

  /* Check if we have space for checkpoint - doesn't work currently due to overflow
  if (sizeof(CPGraph) + num_nodes * sizeof(uint64_t) + num_edges * sizeof(Edge) > CHECKPOINT_SIZE) {
    printf("Write checkpoint: no space\n");
    return NO_SPACE;
  }
  */

  // Lseek to the end of log segment
  uint32_t offset = LOG_START + LOG_SIZE;
  if (lseek(fd, offset, SEEK_SET) != offset) {
    handle_error("could not find checkpoint\n");
  }

  printf("Write checkpoint: at start of checkpoint\n");

  // Mmap CPGraph header and cast 
  CPGraph *header = (CPGraph *) mmap(NULL, sizeof(CPGraph), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (header == MAP_FAILED) {
    handle_error("could not mmap checkpoint header\n");
  }

  printf("Write checkpoint: mmapped header\n");

  // Set info in header
  header->num_nodes = num_nodes;
  header->num_edges = num_edges;
  printf("Write checkpoint: created header with %d nodes and %d edges\n", num_nodes, num_edges);

  // Write out header
  uint32_t bytes_wrote = write(fd, header, sizeof(CPGraph));
  if (bytes_wrote != sizeof(CPGraph)) {
    handle_error("could not write checkpoint header\n");
  }

  printf("Write checkpoint: size of node list %x %x\n", sizeof(nodes), sizeof(nodes[0]));
  printf("Write checkpoint: size of edge list %x %x\n", sizeof(edges), sizeof(edges[0]));

  for (int i = 0; i < num_nodes; i++) {
    uint64_t node = nodes[i];
    if (write(fd, &node, sizeof(node)) != sizeof(node)) {
      handle_error("could not write node\n");
    }
    printf("Write checkpoint: wrote node %llu \n", node);
  }

  for (int i = 0; i < num_edges; i++) {
    Edge *edge = (Edge *) mmap(NULL, sizeof(Edge), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
    if (edge == MAP_FAILED) {
      handle_error("could not mmap checkpoint header\n");
    }
    edge->node_a_id = std::get<0>(edges[i]);
    edge->node_b_id = std::get<1>(edges[i]);
    if (write(fd, edge, sizeof(Edge)) != sizeof(Edge)) {
      handle_error("could not write edge\n");
    }
    printf("Write checkpoint: wrote edge (%llu, %llu) \n", edge->node_a_id, edge->node_b_id);
  }

  return SUCCESS;
}

void read_checkpoint(int fd, Graph *g) {
  printf("Read checkpoint:\n");

  // Go to end of log segment
  uint32_t offset = LOG_START + LOG_SIZE;
  if (lseek(fd, offset, SEEK_SET) != offset) {
    handle_error("could not find checkpoint\n");
  }

  printf("Read checkpoint: at start of checkpoint\n");

  // Mmap CPGraph header and cast 
  CPGraph *header = (CPGraph *) mmap(NULL, sizeof(CPGraph), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (header == MAP_FAILED) {
    handle_error("could not mmap checkpoint header\n");
  }

  printf("Read checkpoint: mmapped header\n");

  // Read in header
  uint32_t bytes_read = read(fd, header, sizeof(CPGraph));
  if (bytes_read != sizeof(CPGraph)) {
    handle_error("could not write checkpoint header\n");
  }

  // Get info from header
  uint64_t num_nodes = header->num_nodes;
  uint64_t num_edges = header->num_edges;
  printf("Read checkpoint: has %d nodes and %d edges\n", num_nodes, num_edges);

  for (int i = 0; i < num_nodes; i++) {
    uint64_t *node = (uint64_t *) mmap(NULL, num_nodes * sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
    if (read(fd, node, sizeof(uint64_t)) != sizeof(uint64_t)) {
      handle_error("could not write checkpoint header\n");
    }
    printf("Read checkpoint: found node %llu\n", *node);
    g->addNode(*node);
  }

  for (int i = 0; i < num_edges; i++) {
    Edge *edge = (Edge *) mmap(NULL, sizeof(Edge), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
    if (edge == MAP_FAILED) {
      handle_error("could not mmap checkpoint header\n");
    }
    if (read(fd, edge, sizeof(Edge)) != sizeof(Edge)) {
      handle_error("could not write checkpoint header\n");
    }
    uint64_t node_a_id = edge->node_a_id;
    uint64_t node_b_id = edge->node_b_id;
    printf("Read checkpoint: found edge (%llu, %llu) \n", node_a_id, node_b_id);
    g->addEdge(node_a_id, node_b_id);
  }
}

void clear_checkpoint(int fd) {
  uint32_t offset = LOG_START + LOG_SIZE;
  if (lseek(fd, offset, SEEK_SET) != offset) {
    handle_error("could not find checkpoint\n");
  }

  // Mmap CPGraph header and cast 
  CPGraph *header = (CPGraph *) mmap(NULL, sizeof(CPGraph), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
  if (header == MAP_FAILED) {
    handle_error("could not mmap checkpoint header\n");
  }

  // Set info in header
  header->num_nodes = 0;
  header->num_edges = 0;
  printf("Clear checkpoint: created header\n");

  // Write out header
  uint32_t bytes_wrote = write(fd, header, sizeof(CPGraph));
  if (bytes_wrote != sizeof(CPGraph)) {
    handle_error("could not write checkpoint header\n");
  }
}

uint32_t play_log(int fd, uint32_t gen_num, Graph *g) {
  uint32_t logtail = 0;

  while (LB_SIZE * logtail < LOG_SIZE) {

    uint32_t offset = LOG_START + (LB_SIZE * logtail);
    if (lseek(fd, offset, SEEK_SET) != offset) {
      handle_error("could not find log tail\n");
    }

    Logblock *logblock = (Logblock *) mmap(NULL, LB_HEADER_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
    if (logblock == MAP_FAILED) {
      handle_error("could not mmap logblock\n");
    }

    // Read header 
    uint32_t bytes_read = read(fd, logblock, LB_HEADER_SIZE);
    if (bytes_read != LB_HEADER_SIZE) {
      handle_error("could not read logblock header\n");
    }

    // If checksum invalid or generation number stale, stop playback
    if (logblock->checksum != compute_lb_checksum(logblock)) {
      handle_error("logblock checksum invalid\n");
    } 
    if (logblock->gen_num != gen_num) {
      break;
    }

    // Loop through entries in this block
    printf("Play log: num entries %d\n", logblock->num_entries);
    for (int i = 0; i < logblock->num_entries; i++) {

      uint32_t entry_offset = LOG_START + (LB_SIZE * logtail) + LB_HEADER_SIZE + (LOG_ENTRY_SIZE * i);
      if (lseek(fd, entry_offset, SEEK_SET) != entry_offset) {
        handle_error("could not find log entry\n");
      }
      LogEntry *entry = (LogEntry *) mmap(NULL, LOG_ENTRY_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
      if (entry == MAP_FAILED) {
        handle_error("could not mmap new entry\n");
      }
      //Read entry
      if (read(fd, entry, LOG_ENTRY_SIZE) != LOG_ENTRY_SIZE) {
        handle_error("could not write new entry\n");
      }
      uint32_t opcode = entry->opcode;
      uint64_t node_a_id = entry->node_a_id;
      uint64_t node_b_id = entry->node_b_id;

      switch(opcode) {
        case ADD_NODE:
          printf("Play log: add node %llu\n", node_a_id);
          g->addNode(node_a_id);
          break;
        case ADD_EDGE:
          printf("Play log: add edge %llu %llu\n", node_a_id, node_b_id);
          g->addEdge(node_a_id, node_b_id);
          break;
        case REMOVE_NODE:
          printf("Play log: remove node %llu\n", node_a_id);
          g->removeNode(node_a_id);
          break;
        case REMOVE_EDGE:
          printf("Play log: remove edge %llu %llu\n", node_a_id, node_b_id);
          g->removeEdge(node_a_id, node_b_id);
          break; 
      }
    }

    // If num entries was max, increment log tail
    if (logblock->num_entries == MAX_LOG_ENTRIES) {
      logtail += 1;
    }
    else {
      break;
    }
  }
  printf("Play log: logtail %d\n", logtail);
  return logtail;
}






