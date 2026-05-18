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
        std::cout << "just so that thisID is used " << thisID << std::endl; 
        // first check if already in local cache 
        auto itr = cache.find(key); 
        // if exists, simply return the cached data
        if (itr != cache.end() && itr->second.flag != INVALID) {
            return itr->second.data[0]; 
        } 
        // else if not cached: 

        // read directory to find data addr         (first memory node) 
        Directory dir = ct->Read(dirptr); 
        remus::rdma_ptr<DataEntry> dataptr = dir.entries[key].ptr; 

        // read the actual data                     (second memory node)
        DataEntry data = ct->Read(dataptr); 

        // cache it locally 
        CacheLine cline{}; 
        cline.flag = SHARED; 
        cline.data[0] = data.value; 
        cache[key] = cline; 

        return data.value; 
    }

    void write(uint64_t key, uint64_t val, CT &ct) {
        // read directory to find data addr         (first memory node)
        Directory dir = ct->Read(dirptr); 
        remus::rdma_ptr<DataEntry> dataptr = dir.entries[key].ptr; 

        // write directory to address               (second memory node)
        DataEntry data; 
        data.value = val; 
        ct->Write(dataptr, data); 
    
        // invalidate local cache entry (if exists)
        auto itr = cache.find(key); 
        if (itr != cache.end()) {
            itr->second.flag = INVALID; 
        }
    }
      
};
