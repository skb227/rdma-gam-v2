#pragma once

#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <unistd.h>
#include <random> 
// #include <vector>

#include <remus/remus.h>
#include "components.h"

using CT = std::shared_ptr<remus::ComputeThread>; 

// #define TIME_US(expr, accumulator) { \
//     auto _t0 = std::chrono::high_resolution_clock::now(); \
//     expr; \
//     auto _t1 = std::chrono::high_resolution_clock::now(); \
//     accumulator += std::chrono::duration_cast<std::chrono::microseconds>(_t1-_t0).count(); \
// }

class GAMcache {

    // ptr to the directory on MN0
    remus::rdma_ptr<DirEntry> dirptr; 

    // this node 
    uint64_t thisID; 

    // acquire lock on data entry 
    remus::rdma_ptr<uint64_t> acquire(remus::rdma_ptr<DirEntry> direntry, CT &ct) {//, std::atomic<uint64_t> &cas_fails) {
        // build lock ptr 
        remus::rdma_ptr<uint64_t> lockptr(direntry.raw() + offsetof(DirEntry, lock)); 
        while (true) {  // loop to keep trying (spin lock) 
            //if (lockptr.compare_exchange_weak(0, 1, ct)) {        // ~ equivalent to tas
            
            if (ct->CompareAndSwap(lockptr, (uint64_t)0, (uint64_t)1)) {
                break; 
            }

            // to always make cas true: 
            // break;
            // to use write instead of cas: 
            // ct->Write(lockptr, (uint64_t)0); 
            // break;
        } 
        return lockptr; 
    }

    // release lock on data entry 
    void release(remus::rdma_ptr<uint64_t> lockptr, CT &ct) {
        ct->Write(lockptr, (uint64_t)0); 
    }

public: 

    GAMcache(uint64_t nodeID, remus::rdma_ptr<DirEntry> dir) 
        : dirptr(dir), thisID(nodeID) {}

    // uint64_t read(uint64_t key, CT &ct, Metrics &m) {
    uint64_t read(uint64_t key, CT &ct) { 
        // to shut compiler up about thisid 
        if (thisID == 1000000) { return 0; }

        // get the ptr to entries[key] DirEntry
        remus::rdma_ptr<DirEntry> direntryptr = dirptr + key;

        // lock the DirEntry 
        auto lockptr = acquire(direntryptr, ct);         // need to get direntryptr
                        // remus::rdma_ptr<uint64_t> lockptr(direntryptr.raw() + offsetof(DirEntry, lock)); 
                        // TIME_US(ct->CompareAndSwap(lockptr, (uint64_t)0, (uint64_t)1), m.cas);  
        DirEntry entry = ct->Read(direntryptr);          // get ptr and version with the lock 
                        // DirEntry entry; 
                        // TIME_US(entry = ct->Read(direntryptr), m.read); 
        DataEntry data = ct->Read(entry.ptr);            // get data value
                        // DataEntry data; 
                        // TIME_US(data = ct->Read(entry.ptr), m.read); 

        // release lock 
        release(lockptr, ct);
                        // TIME_US(ct->Write(lockptr, (uint64_t)0), m.write); 

                        // m.op_cnt++;
                        // m.read_cnt++; 

        // return the data
        return data.value; 
    }

// void write(uint64_t key, uint64_t val, CT &ct, Metrics &m) {
void write(uint64_t key, uint64_t val, CT &ct) {
    // get the ptr to entries[key] DirEntry
    remus::rdma_ptr<DirEntry> direntryptr = dirptr + key;
    
    // lock DirEntry 
    auto lockptr = acquire(direntryptr, ct);
                        // remus::rdma_ptr<uint64_t> lockptr(direntryptr.raw() + offsetof(DirEntry, lock)); 
                        // TIME_US(ct->CompareAndSwap(lockptr, (uint64_t)0, (uint64_t)1), m.cas);  

    // make a new DataEntry 
    DataEntry d{}; 
    d.value = val; 

    // read the DirEntry 
    DirEntry entry = ct->Read(direntryptr);
                        // DirEntry entry; 
                        // TIME_US(entry = ct->Read(direntryptr), m.read);  

    // write the new DataEntry 
    ct->Write(entry.ptr, d); 
                        // TIME_US(ct->Write(entry.ptr, d), m.write);

    // release lock 
    release(lockptr, ct);
                        // TIME_US(ct->Write(lockptr, (uint64_t)0), m.write); 

                        // m.op_cnt++; 
                        // m.write_cnt++; 
    }

};