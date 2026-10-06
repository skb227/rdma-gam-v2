#pragma once

#include <mutex>
#include <atomic> 
#include <vector>

#include <remus/remus.h>


using CT = std::shared_ptr<remus::ComputeThread>; 


// constants
static constexpr uint64_t ENTRIES = 1024;
static constexpr uint64_t OPS = 100000;
static constexpr double READS = 0.5; 
static constexpr uint64_t CACHE_SIZE = ENTRIES;
// static constexpr uint64_t NUM_QUEUES = 4; 
static constexpr uint64_t NUM_NODES = 8; 

// possible states 
enum State {
    UNSHARED,
    SHARED,
    DIRTY,
    INVALID
};

// data entry -- stored (arbitrarily) on MN1
struct DataEntry {
    remus::Atomic<uint64_t> lock;           // lock on data entry 
    uint64_t value;                         // data value 
    uint64_t slist_cnt;                     // # of nodes in slist 
    uint64_t slist[NUM_NODES];                      // nodes that have cached this entry key (as of now, max 8 nodes sharing) 
    uint64_t padding[5];                    // 8 bytes for each field, 64 for 8 uint64_t, 40 (5*8) padding to make 128 bytes total
    // or should i make this 512 bytes also? 

    void init (CT &ct) {
        lock.store(0, ct); 
    }
};

// directory entry -- stored (arbitrarily) on MN0
struct DirEntry {
    uint64_t key;                           // entry's id 
    remus::rdma_ptr<DataEntry> ptr;         // physical addr of data 
    // uint64_t version;                       // versioning number
    // remus::Atomic<uint64_t> version;       // atomic versioning number
    // remus::Atomic<uint64_t> lock;           // lock on data entry 
    // uint64_t slist[2];                      // nodes that have cached this entry key (as of now, max 2 nodes sharing) 
    // uint64_t slist_cnt;                     // # of nodes in slist 
    uint64_t padding[6];                   // 8 bytes for each field, 48 padding (6*8) to make 64 bytes total

    /*
    void init (CT &ct) {
        // version = 0; 
        lock.store(0, ct); 
    }
    */
};

/*
// directory -- array of DirEntry instances, allocated on (arbitrarily) on MN0 
struct Directory {
    // DirEntry entries[ENTRIES]; 
    std::vector<DirEntry> entries{ENTRIES};
};
*/

// cache line entry (exist on remote nodes) 
struct CacheLine {
    State flag;                         // state of the cache line
    uint64_t data[48];                  // cached data 
    remus::rdma_ptr<DataEntry> ptr;     // physical addr of data 
    // uint64_t version;                // cached versioning number 
    uint64_t padding[14];               // enum is 4 bytes plus c++ padding, ptr is 8 bytes, data is 8*48=384, so 14*8 = 112 = 512 total
};

// bucket for cache hash table 
struct Bucket {
    std::unordered_map<uint64_t, CacheLine> entries; 
    std::shared_mutex mtxlock;          // bucket-grain lock 
};

// invalidation table 
struct InvTable {
    uint64_t invbits[ENTRIES];
        // zero-initialized when declared with {}
};

// to pair for set root 
struct Boot {
    remus::rdma_ptr<DirEntry> dirptr; 
    remus::rdma_ptr<remus::rdma_ptr<InvTable>> invarr; 
};



// metrics
struct Metrics {
    uint64_t cas = 0; 
    uint64_t read = 0; 
    uint64_t write = 0; 
    uint64_t op_cnt = 0; 
    uint64_t read_cnt = 0; 
    uint64_t write_cnt = 0; 

    void report(uint64_t node, uint64_t thread) {
        std::cout << "node " << node << " t" << thread << 
                 "\nops: " << op_cnt << "\ncas: " << (op_cnt ? cas / op_cnt : 0) << "\nread: " << (op_cnt ? read / op_cnt : 0) << "\nwrite: " << (op_cnt ? write / op_cnt : 0) << "\n read ops: " << read_cnt << "\nwrite ops: " << write_cnt << std::endl; 
    }
};
