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

    // local cache
    std::vector<Bucket> cache{CACHE_SIZE};

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

    Bucket& getbucket(uint64_t key) {
        return cache[key % CACHE_SIZE];
    }

public: 

    GAMcache(uint64_t nodeID, remus::rdma_ptr<DirEntry> dir) 
        : dirptr(dir), thisID(nodeID) {}

    // uint64_t read(uint64_t key, CT &ct, Metrics &m) {
    uint64_t read(uint64_t key, CT &ct) { 
        // to shut compiler up about thisid 
        if (thisID == 1000000) { return 0; }

        // get cache bucket
        Bucket &buc = getbucket(key);

        // get the ptr to entries[key] DirEntry
        remus::rdma_ptr<DirEntry> direntryptr = dirptr + key;


        // first check if key is cached 

        // get shared lock on bucket 
        std::shared_lock<std::shared_mutex> slock(buc.mtxlock);

        // check if entry exists in bucket
        auto itr = buc.entries.find(key);
        if (itr != buc.entries.end() && itr->second.flag != INVALID) {
            // check VN of cached data to ensure cache correctness coherence (across nodes) 

            // with atomic vn 
            remus::rdma_ptr<DataEntry> dataptr = itr->second.ptr; 
            remus::rdma_ptr<uint64_t> versionptr (dataptr.raw() + offsetof(DataEntry, version)); 
            uint64_t check = ct->Read(versionptr); 
            // is this correct? an atomic read without a lock would interrupt something holding the lock, but it wouldn't be able to access the vn if the writer was in the process of writing to the vn
            // and the vn is written before the value, so technically it'd be out of order... 
            // otherwise i could do like idk maybe two updates on vn during write so first one makes it odd meaning write in progress, so if read finds odd vn knows it is in process of writing and if even 
            //    no write is currently occurring but still have to check match 
            // but again that doesn't necessarily mean that there isn't another write, like a writer could obtain the lock, a reader checks the vn and finds it hasn't changed, and then the writer increments
            //    the vn, so technically out of order 
            // could also just go back to locks... 

            /*
            auto lockptr = acquire(direntryptr, ct);
            remus::rdma_ptr<DataEntry> dataptr = itr->second.ptr; 
            remus::rdma_ptr<uint64_t> versionptr (dataptr.raw() + offsetof(DataEntry, version));
            uint64_t check = ct->Read(versionptr); 
            release(lockptr, ct);
            */

            if (check == itr->second.version) {
                // if versioning number matches 
                return itr->second.data[0]; 
            }
            
            // else -- vn doesn't match -- continue 
            
            // return itr->second.data[0]; 
        }


        // otherwise, not cached or invalid:

        // unlock cache bucket
        slock.unlock(); 

        // get xlock on bucket to add new entry 
        std::unique_lock<std::shared_mutex> xlock(buc.mtxlock); 

        // check cache one more time to ensure another thread / node didn't cache while waiting for xlock 
        // auto itr_check = buc.entries.find(key); 
        // if (itr_check != buc.entries.end() && itr_check->second.flag != INVALID) {
        //     xlock.unlock();
        //     return itr_check->second.data[0]; 
        // }
        // skipping this check for now... 

        
        // lock the DirEntry
        auto lockptr = acquire(direntryptr, ct); 

        // check if cached but invalid -- take the data entry addr 
        remus::rdma_ptr<DataEntry> dataptr; 

        // if cached -- use cached data ptr (never modified) 
        if (itr != buc.entries.end()) {
            dataptr = itr->second.ptr; 
        } else {
        // no cached -- rdma read to DirEntry 
            DirEntry entry = ct->Read(direntryptr); 
            dataptr = entry.ptr; 
        }

        // read the new data entry 
        DataEntry data = ct->Read(dataptr); 
        
/*
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
      */  
        

        // release lock 
        release(lockptr, ct);
                        // TIME_US(ct->Write(lockptr, (uint64_t)0), m.write); 

                        // m.op_cnt++;
                        // m.read_cnt++; 

        // cache the entry 
        CacheLine cline{}; 
        cline.flag = SHARED; 
        cline.data[0] = data.value; 
        // cline.ptr = entry.ptr; 
        cline.ptr = dataptr; 
        cline.version = data.version; 

        // add to bucket 
        buc.entries[key] = cline; 

        // release xlock 
        xlock.unlock(); 

        // return the data
        return data.value; 
    }



// void write(uint64_t key, uint64_t val, CT &ct, Metrics &m) {
void write(uint64_t key, uint64_t val, CT &ct) {
    // get cache bucket 
    Bucket &buc = getbucket(key);

    // get xlock on cache
    std::unique_lock<std::shared_mutex> xlock(buc.mtxlock);

    // check if key is in cache
    auto itr = buc.entries.find(key);
    if (itr != buc.entries.end()) {
        // if cached, invalidate        -- IOW
        // itr->second.flag = INVALID; 

        // if cached, update cache      -- UOW
        itr->second.data[0] = val;
    }

    // get the ptr to entries[key] DirEntry
    remus::rdma_ptr<DirEntry> direntryptr = dirptr + key;
    
    // lock DirEntry 
    auto lockptr = acquire(direntryptr, ct); 

    // holder for DataEntry ptr 
    remus::rdma_ptr<DataEntry> dataptr; 

    // if cached -- use cached data ptr (never modified) 
    
    if (itr != buc.entries.end()) {
        dataptr = itr->second.ptr; 
    } else {
    // not cached -- rdma read to DirEntry 
        DirEntry entry = ct->Read(direntryptr); 
        dataptr = entry.ptr; 

        // make new (invalid) cache entry to store data entry pointer 
        // CacheLine cline{}; 
        // cline.flag = INVALID; 
        // cline.ptr = entry.ptr; 
        // cline.version = entry.version; 
        // buc.entries[key] = cline; 
    }

    // DirEntry entry = ct->Read(direntryptr);
    // dataptr = entry.ptr; 

    // release lock on cache
    xlock.unlock();  

    // using atomic versioning
    remus::rdma_ptr<uint64_t> verptr(dataptr.raw() + offsetof(DataEntry, version)); 
    remus::rdma_ptr<uint64_t> valptr(dataptr.raw() + offsetof(DataEntry, value)); 

    // FAA atomic incerment for VN 
    ct->FetchAndAdd(verptr, 1); 

    // write new value
    ct->Write(valptr, val); 
    
    /*
    // make a new DataEntry 
    DataEntry d{}; 
    d.value = val; 

    // update versioning number
    DataEntry old = ct->Read(dataptr);
    d.version = old.version + 1; 

    // write the new DataEntry 
    ct->Write(dataptr, d); 
    */

    // release lock 
    release(lockptr, ct);       
    
    }

};