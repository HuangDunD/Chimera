# Chimera: Mitigating Ownership Transfers in Multi-Primary Shared-Storage Cloud-Native Databases

**Chimera** is a multi-primary, cloud-native OLTP database that adopts the compute-storage disaggregated architecture. Chimera consists of a shared storage layer that stores data pages and logs, a meta server maintaining the GPLM and GVT, multiple primaries for transaction execution, and a coordinator responsible for scheduling the phase of all nodes. The components of Chimera are interconnected via TCP/IP-based Local-Area-Network (LAN) within a data center. 

- To reduce the frequent page ownership transfer cost, Chimera employs a novel two-phase transaction scheduling mechanism, eliminating ownership transfers for intra-node transactions processed in the partitioned phase.
- Chimera proposes a delay-fetch ownership transfer mechanism that groups the operations accessing the same pages, further reducing the ownership transfers in the global phase.

**Getting started**

1.  Git Clone

```jsx
git clone --recursive https://github.com/HuangDunD/Chimera.git
```

2.  Install bRPC

```jsx
https://github.com/apache/brpc/blob/master/docs/cn/server.md
```

3. Compile Chimera

```jsx
mkdir build
cd build 
cmake ..
make -j16
```

4. run Chimera

```jsx
# run storage_server
cd storage_server && ./storage_pool
# run remote_server
cd remote_server && ./remote_node
# run compute_server (benchmark, system, thread num, coro num, read ratio, local transaction ratio)
cd remote_server && ./compute_server smallbank eager 2 4 0 0.5 
```
