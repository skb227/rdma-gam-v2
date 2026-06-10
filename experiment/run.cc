#include <memory>
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
        // std::cout << "just so that total_threads is used " << total_threads << std::endl; 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
            compute_threads.push_back(std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
        }

        // to get thread 0 on cn0, to create the directory 
        auto &ct0 = compute_threads[0];

        // rdma ptr to directory -- shared
        remus::rdma_ptr<Directory> dirptr; 

        // (prefill data structure)
        // CN 0 will construct the data structure (directory) and save it in root
        if (id == c0) {
            // std::cout << "do i at least get to here" << std::endl; 
            // allocated directory structure on MN0 (supposedly?)
            dirptr = ct0->allocate<Directory>(); 
            
            // allocate N DataEntry slots -- each with own rdma_ptr
            const uint64_t N = ENTRIES;             // number of kv pairs for testing
            remus::rdma_ptr<DataEntry> dataptrs[N];
            for (uint64_t i = 0; i < N; i++) {
                dataptrs[i] = ct0->allocate<DataEntry>(); 
            }

            // write test values to each DataEntry slots 
            for (uint64_t i = 0; i < N; i++) {
                // std::cout << "writing to slot " << i << std::endl; 
                DataEntry d{};
                d.value = (i + 1) * 10;         // key 0 = 10, key 1 = 20, ...
                ct0->Write(dataptrs[i], d);
            }

            // fill in directory 
            Directory dir{};
            for (uint64_t i = 0; i < N; i++) {
                dir.entries[i].key = i; 
                // std::cout << "writing key " << i << " to node " << dataptrs[i].id() << std::endl; 
                dir.entries[i].ptr = dataptrs[i];
            }
            ct0->Write(dirptr, dir); 

            // set directory as the root
            ct0->set_root(dirptr); 

            // std::cout << "directory filled and set as root" << std::endl; 
        }
        
        // barrier -- to ensure CN0 has set the root before any other node gets root 
        ct0->arrive_control_barrier(cn - c0 + 1); 

        // each node reads dirptr 
        dirptr = ct0->get_root<Directory>();

        // each node has a cache 
        auto cache = std::make_shared<GAMcache>(id, dirptr); 

        // make threads and start them
        std::vector<std::thread> worker_threads; 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); i++) {
            // i = 0 is first thread, i = 1 is second thread 
            worker_threads.push_back(std::thread(
                [&](uint64_t i) {
                    // each node has its own compute thread context 
                    auto &ct = compute_threads[i]; 
                    // wait for all threads to be created across all nodes
                    ct->arrive_control_barrier(total_threads);

                    // each thread makes its own file 
                    std::ofstream file("thread" + std::to_string(i) + ".txt", std::ios::out); 
                    // and then wait 
                    ct->arrive_control_barrier(total_threads);

                    // set up random number generator 
                    std::uniform_int_distribution<> dist(0, (ENTRIES)-1); 
                    std::uniform_int_distribution<> dist2(0, 1);
                    std::mt19937 gen(std::random_device{}());

                    // std::cout << "about to start work" << std::endl; 

                    // get starting time before thread does any work 
                    std::chrono::high_resolution_clock::time_point start_thr = std::chrono::high_resolution_clock::now(); 
                    ct->arrive_control_barrier(total_threads);

                    // each thread workload
                    uint64_t num_reads = 0; 
                    uint64_t read_ops = OPS * (READS/100); 
                    for (uint64_t k = 0; k < OPS; k++) {
                        uint64_t op = dist2(gen); 
                        if (op == 0 && num_reads < read_ops) {
                            num_reads++;
                            uint64_t readid = dist(gen); 
                            uint64_t val = cache->read(readid, ct); 
                            file << "read key " << readid << ", val " << val << std::endl; 
                        } else {
                            uint64_t readid = dist(gen); 
                            cache->write(readid, dist(gen)*10, ct); 
                            uint64_t val3 = cache->read(readid, ct); 
                            file << "write, read key " << readid << ", val " << val3 << std::endl; 
                        }
                    }

                    // final barrier 
                    // ct->arrive_control_barrier(cn - c0 + 1); 

                    // get ending time
                    auto end_thread = std::chrono::high_resolution_clock::now(); 
                    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end_thread - start_thr).count(); 
                    std::cout << dur << std::endl; 

                    // std::cout << "past barrier 1, going to construct gamcache" << std::endl; 

                    // first thread of each node will read the root, construct the cache 
                  },
            i));
        }
        for (auto &t : worker_threads) {
            t.join(); 
        } 
    } 
};