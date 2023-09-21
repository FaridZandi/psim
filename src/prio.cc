#include "prio.h"
#include "config.h"
#include <algorithm>
#include "spdlog/spdlog.h"

using namespace psim;


FairSharePriorityAllocator::FairSharePriorityAllocator(double total_available){
    this->total_available = total_available;
    total_registered = 0;
    total_allocated = 0; 
}

FairSharePriorityAllocator::~FairSharePriorityAllocator(){

}

void FairSharePriorityAllocator::reset(){
    total_registered = 0;
    total_allocated = 0; 
}

void FairSharePriorityAllocator::register_rate(int id, double rate, int priority){
    total_registered += rate;
}

void FairSharePriorityAllocator::compute_allocations(){

}

double FairSharePriorityAllocator::get_allocated_rate(int id, double registered_rate, int priority){
    if (total_registered < total_available) {
        total_allocated += registered_rate;
        return registered_rate;
    } else {
        double allocated_rate = registered_rate * total_available / total_registered;
        total_allocated += allocated_rate;
        return allocated_rate;
    }
}









////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////






FixedLevelsPriorityAllocator::FixedLevelsPriorityAllocator(double total_available){
    this->total_available = total_available;
    this->priority_levels = GConf::inst().bn_priority_levels;

    total_registered = 0;
    total_allocated = 0; 

    for (int i = 0; i < priority_levels; i++){
        register_map.push_back(0);
        availability_map.push_back(0);
    }
}

FixedLevelsPriorityAllocator::~FixedLevelsPriorityAllocator(){

}

void FixedLevelsPriorityAllocator::reset(){
    total_registered = 0;
    total_allocated = 0; 

    for (int i = 0; i < priority_levels; i++){
        register_map[i] = 0;
        availability_map[i] = 0;
    }
}

void FixedLevelsPriorityAllocator::register_rate(int id, double rate, int priority){
    if (priority >= priority_levels) {
        priority = priority_levels - 1;
    }

    register_map[priority] += rate;
    total_registered += rate;
}

void FixedLevelsPriorityAllocator::compute_allocations(){
    double available = total_available;

    for (int i = 0; i < priority_levels; i++) {
        bool depleted = (register_map[i] >= available);

        availability_map[i] = std::min(register_map[i], available);
        available -= availability_map[i];

        if (depleted) {
            break;
        }
    }
}

double FixedLevelsPriorityAllocator::get_allocated_rate(int id, double registered_rate, int priority){
    if (priority >= priority_levels) {
        priority = priority_levels - 1;
    }

    double allocated_rate = 0;
    
    if (total_registered < total_available) {
        allocated_rate = registered_rate;
    } else {
        allocated_rate = registered_rate * availability_map[priority] / register_map[priority];
    }

    total_allocated += allocated_rate;

    return allocated_rate;
}








////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////








PriorityQueuePriorityAllocator::PriorityQueuePriorityAllocator(double total_available){
    this->total_available = total_available;
    total_registered = 0;
    total_allocated = 0; 
}

PriorityQueuePriorityAllocator::~PriorityQueuePriorityAllocator(){

}

void PriorityQueuePriorityAllocator::reset(){
    total_registered = 0;
    total_allocated = 0; 

    if (!register_queue.empty()) {
        spdlog::warn("PriorityQueuePriorityAllocator::reset() called with non-empty register_queue");
    }
    allocations.clear();
}

void PriorityQueuePriorityAllocator::register_rate(int id, double rate, int priority){
    total_registered += rate;
    register_queue.push(std::make_pair(-1 * priority, std::make_pair(id, rate)));
}

void PriorityQueuePriorityAllocator::compute_allocations(){
    double available = total_available;

    while (!register_queue.empty()) {
        std::pair<int, std::pair<int, double> > item = register_queue.top();
        register_queue.pop();

        int priority = item.first;
        int id = item.second.first;
        double rate = item.second.second;

        bool depleted = (rate >= available);

        allocations[id] = std::min(rate, available);
        available -= allocations[id];

        if (depleted) {
            while (!register_queue.empty()) {
                register_queue.pop();
            }
            break;
        }
    }

}

double PriorityQueuePriorityAllocator::get_allocated_rate(int id, double registered_rate, int priority){
    if (allocations.find(id) == allocations.end()) {
        return 0;
    } else {
        double allocated = allocations[id];
        total_allocated += allocated;
        return allocated;
    }
}
