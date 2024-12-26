#include "server.h"

void ComputeServer::rpc_run(int try_operation_cnt){

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    int operation_cnt = 0;
    while(true){
        operation_cnt++;
        if(operation_cnt > try_operation_cnt){
            break;
        }
        // 1. 生成要访问的数据页
        auto page_id_pair = GernerateRandomPageID(gen, dis);
        page_id_t page_id = page_id_pair.page_id;
        bool is_read = page_id_pair.operation_type == OperationType::READ;

        // 2. fetch page
        Page* page;
        if(is_read){
            page = rpc_fetch_s_page(page_id);
        } else {
            page = rpc_fetch_x_page(page_id);
        }

        // 读取或操作数据页
        // off_t offset = dis(gen) * PAGE_SIZE;
        // for(int i=0; i<100; i++){
        //     if(is_read){
        //         char* byte = page->get_data() + offset;
        //         if(0) printf("%c", *byte);
        //     }
        //     else{
        //         char* byte = page->get_data() + offset;
        //         *byte = 'a';
        //     }
        // }
        
        // 3. 模拟操作数据页
        std::this_thread::sleep_for(std::chrono::microseconds(RunOperationTime)); // sleep RunOperationTime us

        // 4. 释放数据页
        if(is_read){
            rpc_release_s_page(page_id);
        } else {
            rpc_release_x_page(page_id);
        }

        // 5. 计时
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - page_id_pair.start_time); // us
        node_->latency_mutex.lock();
        node_->latency_vec.push_back(duration.count()); // us
        node_->latency_mutex.unlock();
    }
}

void ComputeServer::rpc_lazy_run(int try_operation_cnt){

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    int operation_cnt = 0;
    while(true){
        operation_cnt++;
        if(operation_cnt > try_operation_cnt){
            break;
        }
        
        // 1. 生成要访问的数据页
        auto page_id_pair = GernerateRandomPageID(gen, dis);
        page_id_t page_id = page_id_pair.page_id;
        bool is_read = page_id_pair.operation_type == OperationType::READ;

        // 2. fetch page
        Page* page;
        if(is_read){
            page = rpc_lazy_fetch_s_page(page_id);
        } else {
            page = rpc_lazy_fetch_x_page(page_id);
        }

        // 读取或操作数据页
        // off_t offset = dis(gen) * PAGE_SIZE;
        // for(int i=0; i<100; i++){
        //     if(is_read){
        //         char* byte = page->get_data() + offset;
        //         if(0) printf("%c", *byte);
        //     }
        //     else{
        //         char* byte = page->get_data() + offset;
        //         *byte = 'a';
        //     }
        // }

        // 3. 模拟操作时间
        std::this_thread::sleep_for(std::chrono::microseconds(RunOperationTime)); // sleep RunOperationTime us

        // 4. 释放数据页
        if(is_read){
            rpc_lazy_release_s_page(page_id);
        } else {
            rpc_lazy_release_x_page(page_id);
        }

        // 5. 计时
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - page_id_pair.start_time); // us
        node_->latency_mutex.lock();
        node_->latency_vec.push_back(duration.count()); // us
        node_->latency_mutex.unlock();
    }
    rpc_lazy_release_all_page();
}

void ComputeServer::rpc_delay_release_run(int try_operation_cnt){
    // 生成check_release_thread
    std::thread check_if_release_thread([this](){
        // 修改线程名称
        prctl(PR_SET_NAME, ("check_thd_n" + std::to_string(node_->node_id)).c_str());
        check_if_release_lock();
    });
    // 生成release_thread
    std::thread release_decided_thread([this](){
        // 修改线程名称
        prctl(PR_SET_NAME, ("release_thd_n" + std::to_string(node_->node_id)).c_str());
        release_decided_lock();
    });
    // 该计算节点运行try_operation_cnt事务
    // 生成线程
    std::vector<std::thread> threads;
    for(int thread_id=0; thread_id<thread_num_per_node; thread_id++){
        // 每个线程运行try_operation_cnt/thread_num_per_node事务
        threads.push_back(std::thread([this, try_operation_cnt, thread_id](){
            // 修改线程名称
            prctl(PR_SET_NAME, ("dy_run_n" + std::to_string(node_->node_id) 
                + "thd_" + std::to_string(thread_id)).c_str());
            delay_release_run_thread(try_operation_cnt/thread_num_per_node, thread_id);
        }));
    }
    // 等待所有线程结束
    for(int i=0; i<thread_num_per_node; i++){
        threads[i].join();
    }
    node_->is_running = false;
    node_->delay_local_page_lock_table->delay_release_list_cv.notify_one();
    // 等待check_if_release_thread结束
    check_if_release_thread.join();
    node_->delay_local_page_lock_table->decided_release_queue_cv.notify_one();
    // 等待release_decided_thread结束
    release_decided_thread.join();
    
    // std::cout << "Node " << node_->node_id << " hit delayed release lock cnt: " 
    //     << node_->delay_local_page_lock_table->hit_delayed_lock_cnt << std::endl;
    node_->hit_delayed_release_lock_cnt += node_->delay_local_page_lock_table->hit_delayed_lock_cnt;
    return;
}

void ComputeServer::rpc_phase_switch_run(int try_operation_cnt){
    // 该计算节点运行try_operation_cnt事务
    
    // 生成check delay release线程和decided release线程
     std::thread check_if_release_thread([this]() {
         // 修改线程名称
         prctl(PR_SET_NAME, ("check_thd_n" + std::to_string(node_->node_id)).c_str());
         check_if_release_lock();
     });
     std::thread release_decided_thread([this]() {
         // 修改线程名称
         prctl(PR_SET_NAME, ("decided_thd_n" + std::to_string(node_->node_id)).c_str());
         release_decided_lock();
     });


    // 生成工作线程
    std::vector<std::thread> threads;
    for(int thread_id=0; thread_id<thread_num_per_node; thread_id++){
        // 每个线程运行try_operation_cnt/thread_num_per_node事务
        // threads.push_back(std::thread(&ComputeServer::phase_switch_run_thread, this, try_operation_cnt/thread_num_per_node, i));
        threads.push_back(std::thread([this, try_operation_cnt, thread_id](){
            // 修改线程名称
            prctl(PR_SET_NAME, ("ps_run_n" + std::to_string(node_->node_id) 
                + "thd_" + std::to_string(thread_id)).c_str());
            phase_switch_run_thread(try_operation_cnt/thread_num_per_node, thread_id);
        }));
    }
    // 生成切换线程
    std::thread switch_thread([this](){
        // 修改线程名称
        prctl(PR_SET_NAME, ("switch_thd_n" + std::to_string(node_->node_id)).c_str());
        switch_phase();
    });
    // 等待所有线程结束
    for(int i=0; i<thread_num_per_node; i++){
        threads[i].join();
    }
    node_->is_running = false;

  if(SYSTEM_MODE == 5) {
      // 通知切换线程
      while (!node_->is_phase_switch_finish || !node_->release_delay_lock_finish ||
             !node_->check_delay_release_finish) {
          node_->phase_switch_cv.notify_one();
          node_->delay_local_page_lock_table->delay_release_list_cv.notify_one();
          node_->delay_local_page_lock_table->decided_release_queue_cv.notify_one();
      }
      // 等待check_if_release_thread结束
      check_if_release_thread.join();
      // 等待release_decided_thread结束
      release_decided_thread.join();
      node_->hit_delayed_release_lock_cnt += node_->delay_local_page_lock_table->hit_delayed_lock_cnt;
  } else {
      // 通知切换线程
      while (!node_->is_phase_switch_finish) {
          node_->phase_switch_cv.notify_one();
      }
  }
    // 等待switch_thread结束
    switch_thread.join();

    return;
}

void ComputeServer::rpc_delay_fetch_run(int try_operation_cnt){

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    int operation_cnt = 0;
    while(true){
        operation_cnt++;
        if(operation_cnt > try_operation_cnt){
            break;
        }
        
        // 1. 生成要访问的数据页
        auto page_id_pair = GernerateRandomPageID(gen, dis);
        page_id_t page_id = page_id_pair.page_id;
        bool is_read = page_id_pair.operation_type == OperationType::READ;

        // 2. fetch page
        Page* page;
        if(is_read){
            page = rpc_delay2_fetch_s_page(page_id);
        } else {
            page = rpc_delay2_fetch_x_page(page_id);
        }

        // 3. 模拟操作时间
        std::this_thread::sleep_for(std::chrono::microseconds(RunOperationTime)); // sleep RunOperationTime us

        // 4. 释放数据页
        if(is_read){
            rpc_delay2_release_s_page(page_id);
        } else {
            rpc_delay2_release_x_page(page_id);
        }

        // 5. 计时
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - page_id_pair.start_time); // us
        node_->latency_mutex.lock();
        node_->latency_vec.push_back(duration.count()); // us
        node_->latency_mutex.unlock();
    }
    rpc_delay_fetch_release_all_page();
}

void ComputeServer::phase_switch_run_thread(int try_operation_cnt, int thread_id){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    int operation_cnt = 0;

    while(node_->phase == Phase::BEGIN);
    while(true){
        if(operation_cnt > try_operation_cnt){
            break;
        }
        // 判断是否需要切换
        if(node_->phase == Phase::SWITCH_TO_GLOBAL || node_->phase == Phase::SWITCH_TO_PAR){
            node_->threads_switch[thread_id] = true; // stop and ready to switch
            while(node_->phase == Phase::SWITCH_TO_GLOBAL || node_->phase == Phase::SWITCH_TO_PAR){
                std::this_thread::sleep_for(std::chrono::microseconds(5)); // sleep 5us
            } // wait for switch
        }
        // 1. fetch a page
        Page_request_info page_id;
        bool fill_page = false;
        if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL){
            // 判断partitioned_page_list是否为空, 如果为空则生成新的page id
            if(!node_->partitioned_page_queue.empty()){
                // 从partitioned_page_list取出一个page id
                fill_page = true;
                page_id = node_->partitioned_page_queue.front();
                node_->partitioned_page_queue.pop();
            }
            else {
                // // 如果当前生成的操作数目已经足够, 则等待另一个队列清空即可
                // if(operation_cnt >= try_operation_cnt) {
                //     std::this_thread::sleep_for(std::chrono::microseconds(10)); // sleep 10us 
                //     continue;
                // }
                // 生成一个partitioned page id
                page_id = GernerateRandomPageID(gen, dis);

                if(!is_partitioned_page(page_id.page_id)){
                    // push back to global_page_list
                    node_->global_page_queue.push(page_id);
                }
                else fill_page = true;
            }
        }
        else if(node_->phase == Phase::GLOBAL || node_->phase == Phase::SWITCH_TO_PAR){
            // 判断global_page_list是否为空, 如果为空则生成新的page id
            if(!node_->global_page_queue.empty()){
                // 从global_page_list取出一个page id
                fill_page = true;
                page_id = node_->global_page_queue.front();
                node_->global_page_queue.pop();
            }
            else {
                // // 如果当前生成的操作数目已经足够, 则等待另一个队列清空即可
                // if(operation_cnt >= try_operation_cnt) {
                //     std::this_thread::sleep_for(std::chrono::microseconds(10)); // sleep 10us 
                //     continue;
                // }
                // 生成一个global page id
                page_id = GernerateRandomPageID(gen, dis);
                if(is_partitioned_page(page_id.page_id)){
                    // push back to partitioned_page_list
                    node_->partitioned_page_queue.push(page_id);
                }
                else fill_page = true;
            }
        }
        else assert(false);
        
        if(!fill_page) continue;

        // 5. 计时
        // page_id.start_time = std::chrono::high_resolution_clock::now(); // 更新计时

        operation_cnt++;
        bool is_read = page_id.operation_type == OperationType::READ;
        Page* page;
        if(is_read){
            page = rpc_phase_switch_fetch_s_page(page_id.page_id);
        }
        else{
            page = rpc_phase_switch_fetch_x_page(page_id.page_id);
        }

        // 2. 读取或操作数据页
        // off_t offset = dis(gen) * PAGE_SIZE;
        // for(int i=0; i<100; i++){
        //     if(is_read){
        //         char* byte = page->get_data() + offset;
        //         if(0) printf("%c", *byte);
        //     }
        //     else{
        //         char* byte = page->get_data() + offset;
        //         *byte = 'a';
        //     }
        // }
        std::this_thread::sleep_for(std::chrono::microseconds(RunOperationTime)); // sleep RunOperationTime us
        
        // 3. release a page
        if(is_read){
            rpc_phase_switch_release_s_page(page_id.page_id);
        }
        else{
            rpc_phase_switch_release_x_page(page_id.page_id);
        }

        // 5. 计时
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - page_id.start_time); // 微秒
        node_->latency_mutex.lock();
        node_->latency_vec.push_back(duration.count()); // us
        node_->latency_pair_vec.push_back(std::make_pair(page_id, duration.count())); // us
        node_->latency_mutex.unlock();
    }
    node_->threads_finish[thread_id] = true;
    return;
}

void ComputeServer::delay_release_run_thread(int try_operation_cnt, int thread_id){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    int operation_cnt = 0;
    while(true){
        operation_cnt++;
        if(operation_cnt > try_operation_cnt){
            break;
        }
        
        // 1. 生成要访问的数据页
        auto page_id_pair = GernerateRandomPageID(gen, dis);
        page_id_t page_id = page_id_pair.page_id;
        bool is_read = page_id_pair.operation_type == OperationType::READ;

        // 2. 申请数据页
        Page* page;
        if(is_read){
            page = rpc_delay_fetch_s_page(page_id);
        } else {
            page = rpc_delay_fetch_x_page(page_id);
        }

        // 读取或操作数据页
        // off_t offset = dis(gen) * PAGE_SIZE;
        // for(int i=0; i<100; i++){
        //     if(is_read){
        //         char* byte = page->get_data() + offset;
        //         if(0) printf("%c", *byte);
        //     }
        //     else{
        //         char* byte = page->get_data() + offset;
        //         *byte = 'a';
        //     }
        // }

        // 3. 模拟操作时间
        std::this_thread::sleep_for(std::chrono::microseconds(RunOperationTime)); // sleep RunOperationTime us

        // 4. 释放数据页
        if(is_read){
            rpc_delay_release_s_page(page_id);
        } else {
            rpc_delay_release_x_page(page_id);
        }

        // 5. 计时
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - page_id_pair.start_time); // us
        node_->latency_mutex.lock();
        node_->latency_vec.push_back(duration.count()); // us
        node_->latency_mutex.unlock();
    }
    return;
}

