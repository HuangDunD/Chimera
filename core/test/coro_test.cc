#include "scheduler/coroutine.h"
#include "scheduler/corotine_scheduler.h"
#include <butil/logging.h>
#include <functional>
#include <iostream>
#include <memory>

using namespace std::placeholders;

CoroutineScheduler* coro_sched;

void RunTest(coro_yield_t& yield, coro_id_t coro_i) {
    // Do some work
    std::cout << "1: coro_i: " << coro_i << std::endl;
    coro_sched->Yield(yield, coro_i);
    std::cout << "2: coro_i: " << coro_i << std::endl;
    coro_sched->Yield(yield, coro_i);
    std::cout << "finished: " << coro_i << std::endl;

    // Stop the coroutine
    coro_sched->StopCoroutine(coro_i);
    while(coro_sched->isAllCoroStopped() == false) {
        coro_sched->Yield(yield, coro_i);
    }
}

int main(){
    int coro_num = 4;
    coro_sched = new CoroutineScheduler(0, coro_num);
    for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
        // Bind workload to coroutine
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTest, _1, coro_i));
    }

    for(coro_id_t coro_i = 0; coro_i < coro_num; coro_i++){
        coro_sched->StartCoroutine(coro_i); // Start all coroutines
    }

    coro_sched->coro_array[0].func();
}
