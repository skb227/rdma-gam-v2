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

class GAMcache {

    // local cache 
    // std::unordered_map<uint64_t, CacheLine> cache; 
    std::array<Bucket, CACHE_SIZE> cache; 
    // std::vector<Bucket> cache{CACHE_SIZE};

    // mtx lock on cache
    // std::shared_mutex mtxlock; 

    // ptr to the directory on MN0
    remus::rdma_ptr<Directory> dirptr; 

    // this node 
    uint64_t thisID; 

    // find the bucket for the key 
    Bucket& getbucket(uint64_t key) {
        return cache[key % CACHE_SIZE]; 
    }

    // acquire lock on data entry 
    remus::rdma_ptr<uint64_t> acquire(remus::rdma_ptr<DirEntry> direntry, CT &ct) {
        // build lock ptr 
        remus::rdma_ptr<uint64_t> lockptr(direntry.raw() + offsetof(DirEntry, lock)); 
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

public: 

    GAMcache(uint64_t nodeID, remus::rdma_ptr<Directory> dir) 
        : dirptr(dir), thisID(nodeID) {}
    
    uint64_t read(uint64_t key, CT &ct) {
        // to shut compiler up about thisid 
        if (thisID == 1000000) { return 0; }

        // get cache bucket
        Bucket &buc = getbucket(key);

        // get the ptr to entries[key] DirEntry
        //      **i don't think i can do pointer arith. if keys weren't sequential and constant? 
        remus::rdma_ptr<DirEntry> direntryptr (dirptr.raw() + offsetof(Directory, entries) + key*sizeof(DirEntry));

        // first need to check if cached 
        
        // get shared lock on bucket 
        std::shared_lock<std::shared_mutex> slock(buc.mtxlock);

        // find if entry exists
        auto itr = buc.entries.find(key);
        if (itr != buc.entries.end() && itr->second.flag != INVALID) {
            // check versioning number -- extra rdma read on each read 
            DirEntry check = ct->Read(direntryptr); 
            if (check.version == itr->second.version) {
                // if versioning number matches 
                return itr->second.data[0]; 
            }
        }

        // else not cached -- release shared lock and acquire writer's lock 
        slock.unlock(); 
        std::unique_lock<std::shared_mutex> xlock(buc.mtxlock); 
        
        // check cache one more time to ensure another thread / node didn't cache while waiting for xlock 
        auto itr_check = buc.entries.find(key); 
        if (itr_check != buc.entries.end() && itr_check->second.flag != INVALID) {
            return itr_check->second.data[0]; 
        }

        // get the ptr to entries[key] DirEntry
        //      **i don't think i can do pointer arith. if keys weren't sequential and constant? 
        // remus::rdma_ptr<DirEntry> direntryptr (dirptr.raw() + offsetof(Directory, entries) + key*sizeof(DirEntry));

        // // read directory to find data addr
        // Directory dir = ct->Read(dirptr); 
        // dataptr = dir.entries[key].ptr; 

        // lock the DirEntry 
        auto lockptr = acquire(direntryptr, ct);         // need to get direntryptr
        DirEntry entry = ct->Read(direntryptr);          // get ptr and version with the lock 
        DataEntry data = ct->Read(entry.ptr);            // get data value

        // release lock 
        release(lockptr, ct);

        // cache the entry 
        CacheLine cline{}; 
        cline.flag = SHARED; 
        cline.data[0] = data.value; 
        cline.ptr = entry.ptr; 
        cline.version = entry.version; 

        // add to bucket 
        buc.entries[key] = cline; 

        // release xlock 
        xlock.unlock(); 

        // return the data
        return data.value; 
    }

void write(uint64_t key, uint64_t val, CT &ct) {
    // get cache bucket
    Bucket &buc = getbucket(key);

    // get xlock on cache
    std::unique_lock<std::shared_mutex> xlock(buc.mtxlock);

    // look for key in cache 
    auto itr = buc.entries.find(key); 
    if (itr != buc.entries.end()) {
        // if cached, invalid 
        itr->second.flag = INVALID; 
    } 

    // release xlock 
    xlock.unlock();         // this isn't a safe idea bc if error thrown above, will never unlock 

    // get the ptr to entries[key] DirEntry
    remus::rdma_ptr<DirEntry> direntryptr (dirptr.raw() + offsetof(Directory, entries) + key*sizeof(DirEntry));
        // could also read this from cache, but why do two things 
    
    // lock DirEntry 
    auto lockptr = acquire(direntryptr, ct); 

    // make a new DataEntry 
    DataEntry d{}; 
    d.value = val; 

    // read the DirEntry 
    DirEntry entry = ct->Read(direntryptr); 

    // make a new DataEntry 
    // DataEntry d{}; 
    // d.value = val; 
    ct->Write(entry.ptr, d); 

    // update versioning number in DirEntry
    entry.version++; 
    ct->Write(direntryptr, entry); 

    // release lock 
    release(lockptr, ct);
    }

};