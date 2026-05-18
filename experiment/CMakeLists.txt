# example
# build the executable
add_executable(run run.cc)
target_link_libraries(run PRIVATE rdma)