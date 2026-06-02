#pragma once

#include <unordered_map>
#include <mutex>
#include <remus/remus.h>
#include "components.h"

using CT = std::shared_ptr<remus::ComputeThread>; 

class GAMcache {

    // local cache
    std::unordered_map<uint64_t, CacheLine> cache; 

    // ptr to the directory on MN0
    remus::rdma_ptr<Directory> dirptr; 

    // this node 
    uint64_t thisID; 

public: 

    GAMcache(uint64_t nodeID, remus::rdma_ptr<Directory> dir) 
        : dirptr(dir), thisID(nodeID) {}
    
    uint64_t read(uint64_t key, CT &ct) {
        // to shut compiler up about thisid 
        if (thisID == 1000000) { return 0; }
    
        // first check if already in local cache 
        auto itr = cache.find(key); 
        // if exists, simply return the cached data
        if (itr != cache.end() && itr->second.flag != INVALID) {
            return itr->second.data[0]; 
        } 
        // else if not cached: 
        // std::cout << "read not cached" << std::endl; 

        // read directory to find data addr         (first memory node) 
        // std::cout << dirptr << std::endl; 
        Directory dir = ct->Read(dirptr); 
        remus::rdma_ptr<DataEntry> dataptr = dir.entries[key].ptr; 

        // std::cout << "reading key " << key << " on dataptr id " << dataptr.id() << std::endl; 

        // std::cout << "read directory to find data addr" << std::endl; 

        // read the actual data                     (second memory node)
        DataEntry data = ct->Read(dataptr); 

        // std::cout << "read actual data" << std::endl; 

        // cache it locally 
        CacheLine cline{}; 
        cline.flag = SHARED; 
        cline.data[0] = data.value; 
        cline.ptr = dataptr; 
        cache[key] = cline; 

        // std::cout << "cached it locally" << std::endl; 

        return data.value; 

        /* for testing without cache */
/*
        Directory dir = ct->Read(dirptr); 
        remus::rdma_ptr<DataEntry> dataptr = dir.entries[key].ptr;

        DataEntry data = ct->Read(dataptr);

        return data.value; 
*/
    }

    void write(uint64_t key, uint64_t val, CT &ct) {
        // data addr 
        remus::rdma_ptr<DataEntry> dataptr; 
        // check if already in local cache 
         auto itr = cache.find(key); 
        // if exists, use the stored addr 
        if (itr != cache.end() && itr->second.flag != INVALID) {
            dataptr = itr->second.ptr; 
        } else {
        // else fetch the addr 
            Directory dir = ct->Read(dirptr); 
            dataptr = dir.entries[key].ptr; 
        }

        // write directory to the address 
        DataEntry data; 
        data.value = val; 
        ct->Write(dataptr, data); 

        // invalidate local cache entry (if exists)
        if (itr != cache.end()) {
            itr->second.flag = INVALID; 
        }

/* for testing without cache */
/*
        // read directory to find data addr         (first memory node)
        Directory dir = ct->Read(dirptr); 
        remus::rdma_ptr<DataEntry> dataptr = dir.entries[key].ptr; 

        // write directory to address               (second memory node)
        DataEntry data; 
        data.value = val; 
        ct->Write(dataptr, data); 
*/
    }
      
};
