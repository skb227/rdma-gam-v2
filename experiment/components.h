#pragma once

#include <mutex>
#include <atomic> 

#include <remus/remus.h>


using CT = std::shared_ptr<remus::ComputeThread>; 

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
    uint64_t version;                       // versioning 
    remus::Atomic<uint64_t> lock;                          // lock on data entry 

    void init(CT & ct) {
        version = 0; 
        lock.store(0, ct); 
    }
};
// directory entry -- stored (arbitrarily) on MN0
struct DirEntry {
    uint64_t key;                           // entry's id 
    remus::rdma_ptr<DataEntry> ptr;         // physical addr of data 
    // remus::Atomic<uint64_t> lock = 0;                      // lock on DataEntry access 
};

// directory -- array of DirEntry instances, allocated on (arbitrarily) on MN0 
struct Directory {
    DirEntry entries[64]; 
};

// cache line entry (exist on remote nodes) 
struct CacheLine {
    State flag;                         // state of the cache line
    uint64_t home_node;                 // home node id
    uint64_t data[64];                  // the cached data 
    remus::rdma_ptr<DataEntry> ptr;     // physical addr of data 
    uint64_t version;  // versioning 
};