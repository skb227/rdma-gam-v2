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

    // ptr to the directory on MN0
    remus::rdma_ptr<DirEntry> dirptr; 

    // this node 
    uint64_t thisID; 

    // local cache
    std::vector<Bucket> cache{CACHE_SIZE};

    // invalidation table  -- for now, a vector where index matches key 
    remus::rdma_ptr<InvTable> invtab; 
    std::unordered_map<uint64_t, remus::rdma_ptr<InvTable>> invmap; 

    // acquire lock on data entry 
    remus::rdma_ptr<uint64_t> acquire(remus::rdma_ptr<DirEntry> direntry, CT &ct) {      //, std::atomic<uint64_t> &cas_fails) {
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

    GAMcache(uint64_t nodeID, remus::rdma_ptr<DirEntry> dir, remus::rdma_ptr<InvTable> invtable, std::unordered_map<uint64_t, remus::rdma_ptr<InvTable>> invmap) 
        : dirptr(dir), thisID(nodeID), invtab(invtable), invmap(invmap) {}

    // uint64_t read(uint64_t key, CT &ct, Metrics &m) {
    uint64_t read(uint64_t key, CT &ct) { 
        // to shut compiler up about thisid 
        if (thisID == 1000000) { return 0; }

        // get cache bucket
        Bucket &buc = getbucket(key);

        // get the ptr to entries[key] DirEntry
        remus::rdma_ptr<DirEntry> direntryptr = dirptr + key;

        uint64_t invalbit = 1; 

        // first check if key is cached 

        // get shared lock on bucket 
        std::shared_lock<std::shared_mutex> slock(buc.mtxlock);

        // check if entry exists in bucket
        auto itr = buc.entries.find(key);
        if (itr != buc.entries.end()) {
            remus::rdma_ptr<uint64_t> invbitptr (invtab.raw() + offsetof(InvTable, invbits) + key*sizeof(uint64_t));
            if (itr->second.flag == INVALID) {
                ct->CompareAndSwap(invbitptr, (uint64_t)1, (uint64_t)0);
            } else {
                uint64_t invbit = ct->CompareAndSwap(invbitptr, (uint64_t)1, (uint64_t)0);

                if (invbit == 0) {
                    slock.unlock(); 
                    return itr->second.data[0]; 
                }
            }
        }
        /*
        if (itr != buc.entries.end() && itr->second.flag != INVALID) {
            // consider CAS when invalid as well, so split up the first if and if invalid still do cas 



            // check invalidation table 
            remus::rdma_ptr<uint64_t> invbitptr (invtab.raw() + offsetof(InvTable, invbits) + key*sizeof(uint64_t));
            invalbit = ct->CompareAndSwap(invbitptr, (uint64_t)1, (uint64_t)0); 

            // if cas result is 0 -- still valid, use cached data
            if (invalbit == 0) {
                slock.unlock();
                return itr->second.data[0]; 
            }

            // else if cas result is 1 -- invalid, need to read from directory 
            // ct->Write(invbitptr, 0); 
            
            // return itr->second.data[0]; 
        }
        */


        // otherwise, not cached or invalid:

        // unlock cache bucket
        slock.unlock(); 

        // get xlock on bucket to add new entry 
        std::unique_lock<std::shared_mutex> xlock(buc.mtxlock);
        
        // lock the DirEntry
        auto lockptr = acquire(direntryptr, ct); 

        // check if cached but invalid -- take the data entry addr 
        remus::rdma_ptr<DataEntry> dataptr; 

        // if cached -- use cached data ptr (never modified) 
        // if (itr != buc.entries.end()) {
        //     dataptr = itr->second.ptr; 
        // } else {
        // // no cached -- rdma read to DirEntry 
        //     DirEntry entry = ct->Read(direntryptr); 
        //     dataptr = entry.ptr; 
        // }

        // need to read the entry for slist details anyway 
        DirEntry entry = ct->Read(direntryptr); 
        dataptr = entry.ptr; 

        // check if this node is already registered in slist 
        bool found = false; 
        for (uint64_t i = 0; i < entry.slist_cnt; i++) {
            if (entry.slist[i] == thisID) {
                found = true; 
                break;
            }
        }

        // need to update slist of DirEntry (if was invalidated or never cached before)
        if ((invalbit == 1 || itr == buc.entries.end()) && found == false) {
            remus::rdma_ptr<uint64_t> cntptr (direntryptr.raw() + offsetof(DirEntry, slist_cnt)); 
            uint64_t slot = ct->FetchAndAdd(cntptr, 1);     // update slist_cnt
            // if (slot < 2) { -- 
                remus::rdma_ptr<uint64_t> slistptr (direntryptr.raw() + offsetof(DirEntry, slist) + slot*sizeof(uint64_t)); 
                ct->Write(slistptr, thisID);
        }

        // read the new data entry 
        DataEntry data = ct->Read(dataptr);  

        // release lock 
        release(lockptr, ct);

        // cache the entry 
        CacheLine cline{}; 
        cline.flag = SHARED; 
        cline.data[0] = data.value; 
        // cline.ptr = entry.ptr; 
        cline.ptr = dataptr; 

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
        itr->second.flag = INVALID; 

        // if cached, update cache      -- UOW
        // itr->second.data[0] = val;
    }

    // get the ptr to entries[key] DirEntry
    remus::rdma_ptr<DirEntry> direntryptr = dirptr + key;
    
    // lock DirEntry 
    auto lockptr = acquire(direntryptr, ct); 

    // holder for DataEntry ptr 
    remus::rdma_ptr<DataEntry> dataptr; 

    // if cached -- use cached data ptr (never modified) 
    
    // if (itr != buc.entries.end()) {
    //     dataptr = itr->second.ptr; 
    // } else {
    // // not cached -- rdma read to DirEntry 
    //     DirEntry entry = ct->Read(direntryptr); 
    //     dataptr = entry.ptr; 
    // }

    // read direntry to get (updated) slist
    DirEntry entry = ct->Read(direntryptr); 
    dataptr = entry.ptr; 
    uint64_t cnt = entry.slist_cnt; 
    
    // release lock on cache
    xlock.unlock();  

    // invalidate each sharing node's inval bit for this key 
    remus::rdma_ptr<uint64_t> cntptr (direntryptr.raw() + offsetof(DirEntry, slist_cnt));
    for (uint64_t i = 0; i < cnt; i++) {
        uint64_t s_node = entry.slist[i]; 
        // if (s_node == thisID) continue; 

        auto itr = invmap.find(s_node); 
        if (itr == invmap.end()) continue; 

        // else write inv bit to 1 
        remus::rdma_ptr<uint64_t> remote_bitptr (itr->second.raw() + offsetof(InvTable, invbits) + key * sizeof(remus::Atomic<uint64_t>)); 
        ct->Write(remote_bitptr, (uint64_t)1); 
    }

    // and then clear the slist cnt
    ct->Write(cntptr, (uint64_t)0); 

    // rdma addr for value 
    remus::rdma_ptr<uint64_t> valptr(dataptr.raw() + offsetof(DataEntry, value)); 

    // write new value
    ct->Write(valptr, val); 

    // invalidate caches on nodes that have previously read (cached) the key 

    // release lock 
    release(lockptr, ct);       
    
    }

};