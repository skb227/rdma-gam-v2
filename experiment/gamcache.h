#pragma once

#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <unistd.h>
#include <random> 

#include <remus/remus.h>
#include "components.h"

using CT = std::shared_ptr<remus::ComputeThread>; 

class GAMcache {

    // local cache 
    std::unordered_map<uint64_t, CacheLine> cache; 

    // mtx lock on cache
    std::shared_mutex mtxlock; 

    // ptr to the directory on MN0
    remus::rdma_ptr<Directory> dirptr; 

    // this node 
    uint64_t thisID; 

    // acquire lock on data entry 
    remus::rdma_ptr<uint64_t> acquire(remus::rdma_ptr<DataEntry> dataptr, CT &ct) {
        // build lock ptr 
        remus::rdma_ptr<uint64_t> lockptr(dataptr.raw()+offsetof(DataEntry, lock)); 
        while (true) {  // loop to keep trying (spin lock) 
            //if (lockptr.compare_exchange_weak(0, 1, ct)) {        // ~ equivalent to tas
            if (ct->CompareAndSwap(lockptr, (uint64_t)0, (uint64_t)1)) {
                break; 
            }
        } 
        return lockptr; 
    }

    // release lock on data entry 
    void release(remus::rdma_ptr<uint64_t> lockptr, CT &ct) {
        ct->Write(lockptr, (uint64_t)0); 
    }

    // for random eviction 
    std::mt19937 gen{std::random_device{}()};

    // random cache eviction 
    void evict() {
        if (cache.empty() || cache.size() <= 0) {
            return; 
        }
        
        // generate random index 
        std::uniform_int_distribution<> dist(0, cache.size() - 1); 
        size_t steps = dist(gen); 
        auto itr = cache.begin(); 
        std::advance(itr, steps); 

        cache.erase(itr); 
    }

public: 

    GAMcache(uint64_t nodeID, remus::rdma_ptr<Directory> dir) 
        : dirptr(dir), thisID(nodeID) {}
    
    uint64_t read(uint64_t key, CT &ct) {
        // to shut compiler up about thisid 
        if (thisID == 1000000) { return 0; }

        // data addr 
        remus::rdma_ptr<DataEntry> dataptr; 

        // acquire reader's lock 
        std::shared_lock<std::shared_mutex> slock(mtxlock);
        // first check if already in local cache 
        auto itr = cache.find(key); 
        // if exists, use the stored addr 
        //if (itr != cache.end() && itr->second.flag != INVALID) {
        if (itr != cache.end()) {
            dataptr = itr->second.ptr; 
            if (itr->second.flag != INVALID) {
                // check that it's the most up-to-date version (versioning values match)
                DataEntry check = ct->Read(dataptr);
                if (check.version  == itr->second.version) {
                    // if versioning matches, return the cached data 
                    return itr->second.data[0]; 
                } // else, continue 
                //std::cout << "versioning mismatch, recache key " << key << std::endl; 
            }
        } 

        // else if not cached: 

        // need to release read lock and grab write lock 
        slock.unlock(); 
        std::unique_lock<std::shared_mutex> xlock(mtxlock);

        // read directory to find data addr         (first memory node) 
        if (itr == cache.end()) {
            Directory dir = ct->Read(dirptr); 
            dataptr = dir.entries[key].ptr; 
        }

        // acquire lock on DataEntry 
        remus::rdma_ptr<uint64_t> lockptr = acquire(dataptr, ct);

        // read the actual data                     (second memory node)
        DataEntry data = ct->Read(dataptr); 

        // release the lock 
        release(lockptr, ct); 

        // cache it locally 
        CacheLine cline{}; 

        cline.flag = SHARED; 
        cline.data[0] = data.value; 
        cline.ptr = dataptr; 
        cline.version = data.version;

        // evict random from cache if needed
        if (cache.size() >= CACHE_SIZE) {
            evict(); 
        }

        // then can safely cache it
        cache[key] = cline; 

        xlock.unlock(); 

        return data.value; 
    }

    void write(uint64_t key, uint64_t val, CT &ct) {
        // data addr 
        remus::rdma_ptr<DataEntry> dataptr; 
        // get xlock 
        std::unique_lock<std::shared_mutex> xlock(mtxlock);
        // check if already in local cache 
        auto itr = cache.find(key); 
        // if exists, use the stored addr 
        //if (itr != cache.end() && itr->second.flag != INVALID) {
        if (itr != cache.end()) {  // invalid flag shouldn't matter for addr -- just to save on rdma reads
            dataptr = itr->second.ptr; 
        } else {
        // else fetch the addr 
            Directory dir = ct->Read(dirptr); 
            dataptr = dir.entries[key].ptr; 
        }

        // acquire lock 
        remus::rdma_ptr<uint64_t> lockptr = acquire(dataptr, ct); 

        // update the version count 
        auto base = dataptr.raw(); 
        ct->FetchAndAdd(rdma_ptr<uint64_t>(base+offsetof(DataEntry, version)), 1);

        // write new value only to value to not overwrite version 
        ct->Write(remus::rdma_ptr<uint64_t>(base+offsetof(DataEntry, value)), val); 

        // release the lock
        release(lockptr, ct); 

        // invalidate local cache entry (if exists)
        if (itr != cache.end()) {
            itr->second.flag = INVALID; 
        }

        xlock.unlock(); 
    }
      
};
