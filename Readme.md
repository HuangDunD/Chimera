<!-- run  -->
mkdir build
cd build 
cmake ..
make
cd storage_server && ./storage_pool
cd remote_server && ./remote_node
cd remote_server && ./compute_server smallbank eager 2 4 0 0.5 