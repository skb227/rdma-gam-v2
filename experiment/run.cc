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
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
            compute_threads.push_back(std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
        }

        // to get thread 0 on cn0, to create the directory 
        auto &ct = compute_threads[0];

        // rdma ptr to directory
        remus::rdma_ptr<DirEntry> dirptr; 
        // rdma ptr to invalidation table array 
        remus::rdma_ptr<remus::rdma_ptr<InvTable>> invarr; 

        // (prefill data structure) -- CN 0 constructs the data structure (directory) and save it in root
        if (id == c0) {
            // allocated directory structure on MN0 (supposedly?)
            // dirptr = ct->allocate<Directory>();
            dirptr = ct->allocate<DirEntry>(ENTRIES); 

            // get ptr to directory 
            // auto dir =std::make_unique<Directory>();

            // initialize empty directory 
            // Directory dir{}; 
            for (uint64_t i = 0; i < ENTRIES; i++) {
                // make a new dir entry object, set its key to i 
                DirEntry d{}; 
                d.key = i; 
                // d.slist_cnt = 0; 

                // let remus do pointer arith 
                ct->Write(dirptr+i, d);
            }

            // set directory as the root 
            // ct->set_root(dirptr); 


            // and set the root invalidation table array
            invarr = ct->allocate<remus::rdma_ptr<InvTable>>(cn - c0 + 1);

            // set Boot as the root 
            remus::rdma_ptr<Boot> boot = ct->allocate<Boot>(); 
            Boot b{}; 
            b.dirptr = dirptr; 
            b.invarr = invarr; 
            ct->Write(boot, b); 
            ct->set_root(boot);       

        } 

        // wait for all nodes 
        ct->arrive_control_barrier(cn - c0 + 1);

        // each node gets the root 
        auto boot = ct->get_root<Boot>(); 
        Boot b = ct->Read(boot);
        dirptr = b.dirptr; 
        invarr = b.invarr; 

        ct->arrive_control_barrier(cn-c0+1);

        // each nodes creates its own data 
        uint64_t numCN = cn - c0 + 1; 
        for (uint64_t i = 0; i < ENTRIES; i++) {
            // only allocate if this should be the home node 
            if (i % numCN == (id - c0)) {           // so only if the value == this id  
                // allocate the entry on this node's memory 
                remus::rdma_ptr<DataEntry> dataptr = ct->allocate<DataEntry>(); 
                        // remus will first try to allocate locally if possible ... i think 

                // write the value 
                DataEntry d{}; 
                d.value = (i + 1) * 10; 
                d.slist_cnt = 0; 
                // d.version = 0; 
                ct->Write(dataptr, d); 
                
                // get rdma ptr to this entry (let remus do pointer arith)
                remus::rdma_ptr<DirEntry> entryptr = dirptr + i; 
                // get rdma ptr to the DataEntry obj (the value) inside that entry 
                remus::rdma_ptr<remus::rdma_ptr<DataEntry>> ptrfield(entryptr.raw() + offsetof(DirEntry, ptr));
                // write the DataEntry made above to that DataEntry in the DirEntry ... 
                ct->Write(ptrfield, dataptr); 
            }
        } 
        
        // each node allocates its own invalidation table
        remus::rdma_ptr<InvTable> inval = ct->allocate<InvTable>(); 
        InvTable thisinv{};         // zero-allocated
        ct->Write(inval, thisinv); 

        // each node writes its own invtab ptr into its slot in invalidation array 
        remus::rdma_ptr<remus::rdma_ptr<InvTable>> this_slot(invarr.raw() + (id-c0)*sizeof(remus::rdma_ptr<InvTable>));
        ct->Write(this_slot, inval);


        ct->arrive_control_barrier(cn-c0+1);

        // then each node reads the inv array to build its local map 
        std::unordered_map<uint64_t, remus::rdma_ptr<InvTable>> invmap; 
        for (uint64_t node = c0; node <= cn; node++) {
            remus::rdma_ptr<remus::rdma_ptr<InvTable>> slot(invarr.raw() + (node - c0)*sizeof(remus::rdma_ptr<InvTable>));
            remus::rdma_ptr<InvTable> node_inval = ct->Read(slot); 
            invmap[node] = node_inval; 
        }

        // wait for all nodes to finish 
        ct->arrive_control_barrier(cn - c0 + 1); 

        // each node has a cache 
        auto cache = std::make_shared<GAMcache>(id, dirptr, inval, invmap); 

        // using metrics for latency rn
        // std::vector<Metrics> metrics(args->uget(remus::CN_THREADS));

        // make threads and start them
        std::vector<std::thread> worker_threads; 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); i++) {
            // i = 0 is first thread, i = 1 is second thread 
            worker_threads.push_back(std::thread(
                [&](uint64_t i) {
                    // create metrics
                    // Metrics &m = metrics[i]; 

                    // each node has its own compute thread context(s)
                    auto &ct = compute_threads[i]; 
                    // wait for all threads to be created across all nodes
                    ct->arrive_control_barrier(total_threads);

                    // each thread makes its own file 
                    // std::ofstream file("thread" + std::to_string(i) + ".txt", std::ios::out); 
                    // and then wait 
                    ct->arrive_control_barrier(total_threads);

                    // to make parallel on one node 
                    // uint64_t tmp = ENTRIES / total_threads;
                    // std::uniform_int_distribution<> dist(tmp*i,tmp*i+tmp-1);

                    // to make parallel across all nodes 
                    // uint64_t tmp = ENTRIES / total_threads;
                    // std::uniform_int_distribution<> dist(tmp*i*(id-c0+1),(tmp*i+tmp-1)*(id-c0+1));

                    // set up random number generator 
                    std::uniform_int_distribution<> dist(0, (ENTRIES)-1); 
                    std::uniform_int_distribution<> dist2(0, 1);
                    std::mt19937 gen(std::random_device{}());

                    // get starting time before thread does any work 
                    ct->arrive_control_barrier(total_threads);
                    std::chrono::high_resolution_clock::time_point start_thr = std::chrono::high_resolution_clock::now(); 

                    // std::chrono::microseconds read_time = {};
                    // std::chrono::microseconds write_time = {}; 

                    // each thread workload -- change for distribution
                    for (uint64_t k = 0; k < OPS/2; k++) {
                        // read 
                            // std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now(); 
                            // cache->read(dist(gen), ct);
                            // std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now(); 
                            // auto time = std::chrono::duration_cast<std::chrono::microseconds>(end - start); 
                            // read_time = read_time + time; 

                            cache->read(dist(gen), ct);
                            
                            // uint64_t readid = dist(gen); 
                            // uint64_t val = cache->read(readid, ct); 
                            // file << "read key " << readid << ", val " << val << std::endl; 

                        // write 
                            // start = std::chrono::high_resolution_clock::now(); 
                            // cache->write(dist(gen), dist(gen)*10, ct); 
                            // cache->write(dist(gen), dist(gen)*10, ct, m); 
                            // end = std::chrono::high_resolution_clock::now(); 
                            // time = std::chrono::duration_cast<std::chrono::microseconds>(end - start); 
                            // write_time = write_time + time; 

                            cache->write(dist(gen), dist(gen)*10, ct);

                            // uint64_t writeid = dist(gen); 
                            // uint64_t val2 = dist(gen)*10;
                            // cache->write(writeid, val2, ct);  
                            // file << "write, key " << writeid << ", val " << val2 << std::endl; 
                    }

                    // get ending time
                    auto end_thread = std::chrono::high_resolution_clock::now(); 
                    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end_thread - start_thr).count(); 

                    // write to file rather than stdout (for script) 
                    std::string filename = "node" + std::to_string(id) + ".txt";
                    std::ofstream res_file(filename); 
                    res_file << "DUR_US:" << dur << std::endl; 
                    std::cout << dur << std::endl; 
                    res_file.close(); 

                    // std::cout << "reads: " << (read_time.count())/10000 << std::endl; 
                    // std::cout << "writes: " << (write_time.count())/10000 << std::endl; 

                    // m.report(id, i); 
                  },
            i));
        }
        for (auto &t : worker_threads) {
            t.join(); 
        } 
    } 
};