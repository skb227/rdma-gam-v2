#include <memory>
#include <vector>
#include <iostream>
#include <chrono>

#include <remus/remus.h>

#include "cloudlab.h"
#include "params.h"
#include "gamcache.h"



void workload(auto ct, uint64_t id, auto cache, std::uniform_int_distribution<> dist, std::uniform_int_distribution<> dist2, std::mt19937 gen) {
    std::chrono::high_resolution_clock::time_point start_thr = std::chrono::high_resolution_clock::now(); 

    uint64_t num_reads = 0; 
    uint64_t num_writes = 0; 

    for (uint64_t k = 0; k < OPS; k++) {
        if (READS != 0 && num_reads < (READS*OPS) && ((dist2(gen) == 0) || num_writes >= ((1-READS)*OPS))) {
            cache->read(((dist(gen) % NUM_NODES) * (id + 1)), ct); 
            num_reads++; 
        } else if (READS != 1.0) {
            cache->write(((dist(gen) % NUM_NODES) * (id + 1)), dist(gen)*10, ct); 
            num_writes++; 
        }
    }

    auto end_thr = std::chrono::high_resolution_clock::now(); 
    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end_thr - start_thr).count(); 

    std::cout << dur << std::endl; 
}


void workload_latency(auto ct, uint64_t id, auto cache, std::uniform_int_distribution<> dist, std::uniform_int_distribution<> dist2, std::mt19937 gen) {
    std::chrono::high_resolution_clock::time_point start_thr = std::chrono::high_resolution_clock::now(); 
    std::chrono::microseconds read_time = {}; 
    std::chrono::microseconds write_time = {}; 

    uint64_t num_reads = 0; 
    uint64_t num_writes = 0; 

    for (uint64_t k = 0; k < OPS; k++) {
        if (READS != 0 && num_reads < (READS*OPS) && ((dist2(gen) == 0) || num_writes >= ((1-READS)*OPS))) {
            auto read_start = std::chrono::high_resolution_clock::now(); 
            cache->read(((dist(gen) % NUM_NODES) * (id + 1)), ct); 
            num_reads++; 
            auto read_end = std::chrono::high_resolution_clock::now();
            read_time += std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start); 
        } else if (READS != 1.0) {
            auto write_start = std::chrono::high_resolution_clock::now(); 
            cache->write(((dist(gen) % NUM_NODES) * (id + 1)), dist(gen)*10, ct); 
            num_writes++; 
            auto write_end = std::chrono::high_resolution_clock::now(); 
            write_time += std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start); 
        }
    }

    auto end_thr = std::chrono::high_resolution_clock::now(); 
    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end_thr - start_thr).count(); 

    std::cout << dur << std::endl; 

    auto read_latency = read_time / num_reads; 
    auto write_latency = write_time / num_writes;
    std::cout << "read time: " << read_time << " OPS: " << OPS << " num_reads: " << num_reads << std::endl; 
    std::cout << "read latency: " << read_latency << "\nwrite latency: " << write_latency << std::endl; 
}


void microbenchmark1(auto ct, uint64_t id, auto cache, std::uniform_int_distribution<> dist, std::uniform_int_distribution<> dist2, std::mt19937 gen) {
    // one node, one thread, 100'000 ops of LOCAL reads and writes

    std::cout << "using params id " << id << ", " << dist(gen) << " and " << dist2(gen) << std::endl; 

    std::chrono::microseconds time_read = {};

    // read once to put into cache 
    cache->read(10, ct); 

    // read 100'000 times 
    for (int i = 0; i < 100000; i++) {
        auto s = std::chrono::high_resolution_clock::now(); 
        cache->read(10, ct); 
        auto e = std::chrono::high_resolution_clock::now();
        time_read += std::chrono::duration_cast<std::chrono::microseconds>(e - s);
    }

    std::chrono::microseconds read_latency = time_read / 100000;
    std::chrono::microseconds time_write = {}; 

    // write once to put into cache (unnecessary but still) 
    cache->write(10, 0, ct);

    // write 100'000 times 
    for (int i = 0; i < 100000; i++) {
        auto s = std::chrono::high_resolution_clock::now(); 
        cache->write(10, 0, ct); 
        auto e = std::chrono::high_resolution_clock::now();
        time_write += std::chrono::duration_cast<std::chrono::microseconds>(e - s);
    }

    std::chrono::microseconds write_latency = time_write / 100000; 

    std::cout << "read latency:  " << read_latency << std::endl; 
    std::cout << "write latency: " << write_latency << std::endl; 
}

void microbenchmark2(auto ct, uint64_t id, auto cache, std::uniform_int_distribution<> dist, std::uniform_int_distribution<> dist2, std::mt19937 gen) {
    // one node, one thread, 100'000 ops of REMOTE reads and writes

    std::cout << "using params id " << id << ", " << dist(gen) << " and " << dist2(gen) << std::endl; 

    std::chrono::microseconds time_read = {};

    // invalidate in into cache 
    cache->read(10, ct);
    cache->invalidate(10, ct); 

    // read (and invalidate) 100'000 times 
    for (int i = 0; i < 100000; i++) {
        auto s = std::chrono::high_resolution_clock::now(); 
        cache->read(10, ct); 
        auto e = std::chrono::high_resolution_clock::now();
        time_read += std::chrono::duration_cast<std::chrono::microseconds>(e - s);
        cache->invalidate(10, ct);
    }

    std::chrono::microseconds read_latency = time_read / 100000;
    std::chrono::microseconds time_write = {}; 

    // write once to put into cache (unnecessary but still) 
    cache->write(10, 0, ct);
    cache->invalidate(10, ct); 

    // write 100'000 times 
    for (int i = 0; i < 100000; i++) {
        auto s = std::chrono::high_resolution_clock::now(); 
        cache->write(10, 0, ct); 
        auto e = std::chrono::high_resolution_clock::now();
        time_write += std::chrono::duration_cast<std::chrono::microseconds>(e - s);
        cache->invalidate(10, ct); 
    }

    std::chrono::microseconds write_latency = time_write / 100000; 

    std::cout << "read latency:  " << read_latency << std::endl; 
    std::cout << "write latency: " << write_latency << std::endl; 
}