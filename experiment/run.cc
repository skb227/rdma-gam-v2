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
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
            compute_threads.push_back(std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
        }

        std::cout << "build the ptrs" << std::endl; 

        // declare ptr for mailbox to be accessible outside assignment
        remus::rdma_ptr<Message> mailptr; 
        // and for the root data structure
        remus::rdma_ptr<GAddr> rootdata; 
        // and for a test data entry 
        remus::rdma_ptr<DataEntry> testdata_ptr; 

        // CN 0 will construct the data structure (mailbox array) and save it in root
        if (id == c0) {
            std::cout << "constructing the data structure" << std::endl; 
            // allocate mailbox -- one Message slot per node 
            mailptr = compute_threads[0]->allocate<Message>(cn - c0 + 1); 
            // initialize all slots in mailbox to invalid 
            for (uint64_t n = 0; n < (cn - c0 + 1); n++) {
                Message empty{}; 
                empty.valid = false; 
                auto slot = remus::rdma_ptr<Message>(
                    mailptr.id(), mailptr.address() + n * sizeof(Message) 
                );
                compute_threads[0]->Write(slot, empty); 
            }

            std::cout << "allocating and initializing data entry" << std::endl; 
            // allocate and initialize test DataEntry 
            testdata_ptr = compute_threads[0]->allocate<DataEntry>(); 
            DataEntry entry{}; 
            entry.data[0] = 42;
            entry.dir.flag = UNSHARED; 
            entry.dir.slist_cnt = 0; 
            entry.dir.dlist[0] = (uint64_t)-1; 
            entry.homeNode = id;
            compute_threads[0]->Write(testdata_ptr, entry); 

            // put both into the root data structure, set as root 
            rootdata = compute_threads[0]->allocate<GAddr>(); 
            GAddr root{}; 
            root.mailboxes = mailptr; 
            root.testdata = testdata_ptr; 
            compute_threads[0]->Write(rootdata, root); 
            compute_threads[0]->set_root(rootdata);
        }
        
        // declare shared ptr for cache (null)
        std::shared_ptr<GAMcache> cache; 
        GAddr gaddr{};

        // make threads and start them
        std::vector<std::thread> worker_threads; 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); i++) {
            worker_threads.push_back(std::thread(
                [&](uint64_t i) {
                    // each node has its own compute thread context 
                    auto &ct = compute_threads[i]; 
                    // wait for all threads to be created across all nodes
                    ct->arrive_control_barrier(total_threads);

                    std::cout << "past barrier 1, going to construct gamcache" << std::endl; 

                    // first thread of each node will read the root, construct the cache 
                    if (i == 0) {
                        // every node reads the root, makes a local reference to it
                        std::cout << "about to read root data" << std::endl; 
                        auto root = ct->get_root<GAddr>(); 
                        gaddr = ct->Read(root);
                        std::cout << "read root data" << std::endl; 
                        auto mbox_base = gaddr.mailboxes; 

                        std::cout << "building cache constructor" << std::endl; 
                        // call constructor for GAMcache
                        cache = std::make_shared<GAMcache>(id, mbox_base);
                    }
                    std::cout << "does the new barrier exist too" << std::endl; 
                    ct->arrive_control_barrier(total_threads); 
                    
                    auto testdata_ptr = gaddr.testdata; 
                    std::cout << "testdata_ptr: " << testdata_ptr.id() << ", " << testdata_ptr.address() << std::endl; 

                    ct->arrive_control_barrier(total_threads); 
                    std::cout << "past latest barrier" << std::endl; 

                    // first thread of each compute node will be reserved for polling 
                    if (i == 0) {
                        std::cout << "polling thread, " << id << ", " << i << std::endl; 
                        // for now skipping polling, just test local reads 
                        // so local read test: 
                        //uint64_t res = cache.read(testdata_ptr, ct); 
                        cache->pollMailbox(ct); 
                    } else {
                        std::cout << "not polling thread, " << id << ", " << i << std::endl; 

                        // local read from node 1 to node 1 
                        if (id == c0) {
                            uint64_t res = cache->read(testdata_ptr, ct); 
                            std::cout << "local read result: " << res << std::endl; 
                        }
                        // remote read from node 2 to node 1 
                        if (id == c0+1) {
                            std::cout << "requesting reading node " << id << std::endl; 
                            std::cout << testdata_ptr.id() << ", " << testdata_ptr.address() << std::endl; 
                            uint64_t res = cache->read(testdata_ptr, ct); 
                            std::cout << "remote read result: " << res << std::endl; 
                            std::cout << "remote read again (cache hit?)"; 
                            uint64_t res2 = cache->read(testdata_ptr, ct); 
                            std::cout << "remote read 2 result: " << res2 << std::endl; 
                        }
                    }

                    std::cout << "thread " << i << " on node " << id << " about to hit the barrier, total=" << total_threads << std::endl; 
                    ct->arrive_control_barrier(total_threads); 
                },
            i));
        }
        for (auto &t : worker_threads) {
            t.join(); 
        }
    } 
};
