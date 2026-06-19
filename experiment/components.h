#pragma once

#include <mutex>
#include <atomic> 

#include <remus/remus.h>


using CT = std::shared_ptr<remus::ComputeThread>; 


// constants
static constexpr uint64_t ENTRIES = 128; 
static constexpr uint64_t OPS = 20000;
static constexpr uint64_t READS = 50;
static constexpr uint64_t CACHE_SIZE = 64;
// static constexpr uint64_t NUM_QUEUES = 4; 

// possible states 
enum State {
    UNSHARED,
    SHARED,
    DIRTY,
    INVALID
};

// data entry -- stored (arbitrarily) on MN1
struct DataEntry {
    uint64_t value;                         // data value 
};

// directory entry -- stored (arbitrarily) on MN0
struct DirEntry {
    uint64_t key;                           // entry's id 
    remus::rdma_ptr<DataEntry> ptr;         // physical addr of data 
    uint64_t version;                       // versioning number
    remus::Atomic<uint64_t> lock;           // lock on data entry 

    void init (CT &ct) {
        version = 0; 
        lock.store(0, ct); 
    }
};

// directory -- array of DirEntry instances, allocated on (arbitrarily) on MN0 
struct Directory {
    DirEntry entries[ENTRIES]; 
};

// cache line entry (exist on remote nodes) 
struct CacheLine {
    State flag;                         // state of the cache line
    uint64_t data[64];                  // cached data 
    remus::rdma_ptr<DataEntry> ptr;     // physical addr of data 
    uint64_t version;                   // cached versioning number 
};