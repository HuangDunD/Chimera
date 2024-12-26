#include "delay_fetch_lock.h"

LockMode DYFetchLocalPageLock::LockExclusive(CoroutineScheduler* coro_sched, coro_yield_t&yield, coro_id_t coro_id, bool delay) {
    // LOG(INFO) << "LockShared: " << page_id;
    bool try_latch = true;
    bool enter = false;
    bool first_enter = true;
    LockMode fetch_mode = LockMode::NONE; //如果返回LockMode::NONE, 则不需要去远程获取控制权
    while(try_latch){
        mutex.lock(); 
        // if(!enter && !is_pending && !is_granting) {
        if(!enter && !is_pending) {
            // 允许进入
            enter = true;
            x_ref_delay_fetch++;
            page_table->delay_fetch_lock_ref++;
        // }else if(!enter && (is_pending || is_granting)){
        }else if(!enter && is_pending){
            mutex.unlock();
            coro_sched->Yield(yield, coro_id);
            continue; // !这里考虑协程切换
        }
        // enter is true
        if(remote_mode == LockMode::EXCLUSIVE){
            if(first_enter){
                page_table->hold_period_hit_ref++;
                first_enter = false;
            }
            if(lock != 0) {
                mutex.unlock();
            }
            else {
                lock = EXCLUSIVE_LOCKED;
                // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                // 加上锁了
                x_ref_delay_fetch--;
                mutex.unlock();
                try_latch = false;
            }
        }
        else if(remote_mode == LockMode::SHARED || remote_mode == LockMode::NONE){
            if(is_granting) {
                // 已经有线程在远程获取锁
                if(first_enter){
                    page_table->rpc_hit_ref++;
                    first_enter = false;
                }
                // do nothing
                mutex.unlock();
                // std::this_thread::sleep_for(std::chrono::microseconds(1)); //sleep 1us
                coro_sched->Yield(yield, coro_id);
                continue;
            }
            else if(!is_in_delay_fetch_list) {
                if(first_enter){
                    page_table->delay_hit_ref++;
                    first_enter = false;
                }
                is_in_delay_fetch_list = true;
                first_fetch_time = std::chrono::steady_clock::now();
                ddl_time = first_fetch_time + std::chrono::microseconds(delay_time);
                assert(delay_fetch_mode == LockMode::NONE);
                delay_fetch_mode = LockMode::EXCLUSIVE;
                mutex.unlock();
                // 这里需要yield
                if(delay_time !=0 && delay) coro_sched->Yield(yield, coro_id);
            }
            else{
                if(first_enter){
                    page_table->delay_hit_ref++;
                    first_enter = false;
                }
                assert(delay_fetch_mode != LockMode::NONE);
                assert(x_ref_delay_fetch > 0);
                delay_fetch_mode = LockMode::EXCLUSIVE;
                if((ddl_time < std::chrono::steady_clock::now() || page_table->phase == Phase::SWITCH_TO_PAR || !delay || s_ref_delay_fetch + x_ref_delay_fetch > KKK) // 超时或在阶段切换中
                        && is_granting == false){
                    // 超时， 去远程获取数据页
                    // page_table->delay_fetch_lock_ref += s_ref_delay_fetch;
                    // page_table->delay_fetch_lock_ref += x_ref_delay_fetch;
                    page_table->delay_fetch_lock_remote++;
                    is_granting = true;
                    try_latch = false;
                    fetch_mode = delay_fetch_mode;
                    is_in_delay_fetch_list = false;
                    mutex.unlock();
                }
                else{
                    mutex.unlock();
                    coro_sched->Yield(yield, coro_id);
                }
            }
        }
        else{
            assert(false);
        }
    }
    return fetch_mode;
}

LockMode DYFetchLocalPageLock::LockShared(CoroutineScheduler* coro_sched, coro_yield_t&yield, coro_id_t coro_id, bool delay) {
    // LOG(INFO) << "LockShared: " << page_id;
    bool try_latch = true;
    bool enter = false;
    bool first_enter = true;
    LockMode fetch_mode = LockMode::NONE; //如果返回LockMode::NONE, 则不需要去远程获取控制权
    while(try_latch){
        mutex.lock(); 
        // if(!enter && !is_pending && !is_granting) {
        if(!enter && !is_pending) {
            // 允许进入
            enter = true;
            s_ref_delay_fetch++;
            page_table->delay_fetch_lock_ref++;
        // }else if(!enter && (is_pending || is_granting)){
        }else if(!enter && is_pending ){
            mutex.unlock();
            coro_sched->Yield(yield, coro_id);
            continue; // !这里考虑协程切换
        }
        // enter is true
        if(remote_mode == LockMode::EXCLUSIVE){
            if(first_enter){
                page_table->hold_period_hit_ref++;
                first_enter = false;
            }
            if(lock == EXCLUSIVE_LOCKED) {
                mutex.unlock();
            }
            else {
                lock++;
                // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                // 加上锁了
                s_ref_delay_fetch--;
                mutex.unlock();
                try_latch = false;
            }
        }
        else if(remote_mode == LockMode::SHARED){
            if(first_enter){
                page_table->hold_period_hit_ref++;
                first_enter = false;
            }
            if(lock == EXCLUSIVE_LOCKED) {
                mutex.unlock();
                LOG(ERROR) << "Locol Grant Exclusive Lock, however remote only grant shared lock";
            }
            else {
                lock++;
                // 由于远程已经持有共享锁, 因此无需再去远程获取锁
                // 加上锁了
                s_ref_delay_fetch--;
                mutex.unlock();
                try_latch = false;
            }
        }
        else if(remote_mode == LockMode::NONE){
            if(is_granting) {
                // 已经有线程在远程获取锁
                if(first_enter){
                    page_table->rpc_hit_ref++;
                    first_enter = false;
                }
                // do nothing
                mutex.unlock();
                // std::this_thread::sleep_for(std::chrono::microseconds(1)); //sleep 1us
                coro_sched->Yield(yield, coro_id);
                continue;
            }
            else if(!is_in_delay_fetch_list) {
                if(first_enter){
                    page_table->delay_hit_ref++;
                    first_enter = false;
                }
                is_in_delay_fetch_list = true;
                first_fetch_time = std::chrono::steady_clock::now();
                ddl_time = first_fetch_time + std::chrono::microseconds(delay_time);
                assert(delay_fetch_mode == LockMode::NONE);
                delay_fetch_mode = LockMode::SHARED;
                mutex.unlock();
                // 这里需要yield
                if(delay_time !=0 && delay) coro_sched->Yield(yield, coro_id);
            }
            else{
                if(first_enter){
                    page_table->delay_hit_ref++;
                    first_enter = false;
                }
                assert(delay_fetch_mode != LockMode::NONE);
                assert(s_ref_delay_fetch > 0);
                if((ddl_time < std::chrono::steady_clock::now() 
                        || page_table->phase == Phase::SWITCH_TO_PAR || !delay || s_ref_delay_fetch + x_ref_delay_fetch > KKK) // 超时或在阶段切换中
                        && is_granting == false){
                    // 超时， 去远程获取数据页
                    // page_table->delay_fetch_lock_ref += s_ref_delay_fetch;
                    // page_table->delay_fetch_lock_ref += x_ref_delay_fetch;
                    page_table->delay_fetch_lock_remote++;
                    is_granting = true;
                    try_latch = false;
                    fetch_mode = delay_fetch_mode;
                    is_in_delay_fetch_list = false;
                    mutex.unlock();
                }
                else{
                    mutex.unlock();
                    coro_sched->Yield(yield, coro_id);
                }
            }
        }
        else{
            assert(false);
        }
    }
    return fetch_mode;
};