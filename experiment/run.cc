#include <memory>
#include <unistd.h>
#include <vector>

#include <iostream>

#include <remus/remus.h>

#include "cloudlab.h"
#include "params.h"
#include "gamcache.h"

int main (int argc, char **argv) {

    // initialize remus
    remus::INIT(); 

    // configure and parse arguments
    auto args = std::make_shared<remus::ArgMap>(); 
    args->import(remus::ARGS);
    args->import(DS_EXP_ARGS);
    args->parse(argc, argv);

    // extract args needed in every node 
    uint64_t id = args->uget(remus::NODE_ID);
    uint64_t m0 = args->uget(remus::FIRST_MN_ID); 
    uint64_t mn = args->uget(remus::LAST_MN_ID);
    uint64_t c0 = args->uget(remus::FIRST_CN_ID);
    uint64_t cn = args->uget(remus::LAST_CN_ID);

    // prepare network info about this machine and about memory nodes
    remus::MachineInfo self(id, id_to_dns_name(id)); 
    std::vector<remus::MachineInfo> memnodes; 
    for (uint64_t i = m0; i <= mn; ++i) {
        memnodes.emplace_back(i, id_to_dns_name(i)); 
    }

    // info for memory node
    std::unique_ptr<remus::MemoryNode> memory_node; 

    // info for compute node
    std::shared_ptr<remus::ComputeNode> compute_node;

    // if memory node, configure to be memory node
    if (id >= m0 && id <= mn) {
        // make the pools, await connections 
        memory_node.reset(new remus::MemoryNode(self, args));
    }

    // if compute node
    if (id >= c0 && id <= cn) {
        compute_node.reset(new remus::ComputeNode(self, args)); 
        // if this CN is also a MN, then need to pass the rkeys to the local MN
        //  there's no harm in doing them first
        if (memory_node.get() != nullptr) {
            auto rkeys = memory_node->get_local_rkeys(); 
            compute_node->connect_local(memnodes, rkeys);
        }
        compute_node->connect_remote(memnodes);
    }

    // if memory node, pause until it's received all expected connections
    //      then spin until control channel in each segment is 1
    //      then shutdown memory node
    if (memory_node) {
        memory_node->init_done(); 
    }

    // if compute node, create threads and run experiment 
    if (id >= c0 && id <= cn) { 
        // create ComputeThread contexts
        std::vector<std::shared_ptr<remus::ComputeThread>> compute_threads; 
        uint64_t total_threads = (cn - c0 + 1) * args->uget(remus::CN_THREADS); 
        std::cout << "just so that total_threads is used " << total_threads << std::endl; 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
            compute_threads.push_back(std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
        }

        // BECAUSE THIS IS CURRENTLY SINGLE-THREADED :::
        auto &ct = compute_threads[0];

        // rdma ptr to directory -- shared
        remus::rdma_ptr<Directory> dirptr; 

        // CN 0 will construct the data structure (directory) and save it in root
        if (id == c0) {
            // allocated directory structure on MN0 
            dirptr = ct->allocate<Directory>(); 
            
            // allocate N DataEntry slots -- each with own rdma_ptr
            const uint64_t N = 4;             // number of kv pairs for testing
            remus::rdma_ptr<DataEntry> dataptrs[N];
            for (uint64_t i = 0; i < N; i++) {
                dataptrs[i] = ct->allocate<DataEntry>(); 
            }

            // write test values to each DataEntry slots 
            for (uint64_t i = 0; i < N; i++) {
                DataEntry d{};
                d.value = (i + 1) * 10;         // key 0 = 10, key 1 = 20, ... 
                ct->Write(dataptrs[i], d);
            }

            // fill in directory 
            Directory dir{};
            for (uint64_t i = 0; i < N; i++) {
                dir.entries[i].key = i; 
                dir.entries[i].ptr = dataptrs[i];
            }
            ct->Write(dirptr, dir); 

            // set directory as the root
            ct->set_root(dirptr); 
        }
        
        // barrier -- to ensure CN0 has set the root before any other node gets root 
        ct->arrive_control_barrier(cn - c0 + 1); 

        // every node gets the root directory 
        dirptr = ct->get_root<Directory>(); 

        // each node constructs the cache
        GAMcache cache(id, dirptr); 

        // test reads
        for (uint64_t i = 0; i < 4; i++) {
            uint64_t val = cache.read(i, ct); 
            std::cout << "read key = " << i << " --> value = " << val << " (expected " << (i+1)*10 << ")" << std::endl; 
        }

        // test cache hit (read the same)
        uint64_t val2 = cache.read(0, ct); 
        std::cout << "cache hit read key = 0 --> value = " << val2 << std::endl; 

        // test write and then read 
        if (id == c0) {
            std::cout << "node " << id << ": writing key=0 as 999" << std::endl; 
            cache.write(0, 999, ct); 
            uint64_t val3 = cache.read(0, ct); 
            std::cout << "after write, read key = 0 --> value = " << val3 << std::endl; 
        }

        // final barrier 
        ct->arrive_control_barrier(cn - c0 + 1); 
        
        // make threads and start them
        // std::vector<std::thread> worker_threads; 
        // for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); i++) {
        //     worker_threads.push_back(std::thread(
        //         [&](uint64_t i) {
        //             // each node has its own compute thread context 
        //             auto &ct = compute_threads[i]; 
        //             // wait for all threads to be created across all nodes
        //             ct->arrive_control_barrier(total_threads);

        //             std::cout << "past barrier 1, going to construct gamcache" << std::endl; 

        //             // first thread of each node will read the root, construct the cache 
        //           },
        //     i));
        // }
        // for (auto &t : worker_threads) {
        //     t.join(); 
        // }
    } 
};