#include "bwalloc.h"
#include "config.h"
#include <algorithm>
#include "spdlog/spdlog.h"

using namespace psim;


void BandwidthAllocator::reset() {
    total_registered = 0;
    total_allocated = 0; 
    utilized_bandwidth = 0;
}

void BandwidthAllocator::register_utilization(double utilization) {
    utilized_bandwidth += utilization;

}

FairShareBandwidthAllocator::FairShareBandwidthAllocator(double total_available){
    this->total_available = total_available;
    BandwidthAllocator::reset(); 
}

FairShareBandwidthAllocator::~FairShareBandwidthAllocator(){

}

void FairShareBandwidthAllocator::reset(){
    BandwidthAllocator::reset(); 
}

void FairShareBandwidthAllocator::register_rate(int id, double rate, int priority){
    total_registered += rate;
}

void FairShareBandwidthAllocator::compute_allocations(){

}

double FairShareBandwidthAllocator::get_allocated_rate(int id, double registered_rate, int priority){
    if (total_registered <= total_available) {
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




FixedLevelsBandwidthAllocator::FixedLevelsBandwidthAllocator(double total_available){
    this->total_available = total_available;
    this->priority_levels = GConf::inst().bn_priority_levels;

    BandwidthAllocator::reset(); 

    for (int i = 0; i < priority_levels; i++){
        register_map.push_back(0);
        availability_map.push_back(0);
    }
}

FixedLevelsBandwidthAllocator::~FixedLevelsBandwidthAllocator(){

}

void FixedLevelsBandwidthAllocator::reset(){
    BandwidthAllocator::reset(); 

    for (int i = 0; i < priority_levels; i++){
        register_map[i] = 0;
        availability_map[i] = 0;
    }
}

void FixedLevelsBandwidthAllocator::register_rate(int id, double rate, int priority){
    if (priority >= priority_levels) {
        priority = priority_levels - 1;
    }

    register_map[priority] += rate;
    total_registered += rate;
}

void FixedLevelsBandwidthAllocator::compute_allocations(){
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

double FixedLevelsBandwidthAllocator::get_allocated_rate(int id, double registered_rate, int priority){
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








PriorityQueueBandwidthAllocator::PriorityQueueBandwidthAllocator(double total_available){
    this->total_available = total_available;
    BandwidthAllocator::reset(); 
}

PriorityQueueBandwidthAllocator::~PriorityQueueBandwidthAllocator(){

}

void PriorityQueueBandwidthAllocator::reset(){
    BandwidthAllocator::reset(); 

    if (!register_queue.empty()) {
        spdlog::warn("PriorityQueueBandwidthAllocator::reset() called with non-empty register_queue");
    }
    allocations.clear();
}

void PriorityQueueBandwidthAllocator::register_rate(int id, double rate, int priority){
    total_registered += rate;
    register_queue.push(std::make_pair(-1 * priority, std::make_pair(id, rate)));
}

void PriorityQueueBandwidthAllocator::compute_allocations(){
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

double PriorityQueueBandwidthAllocator::get_allocated_rate(int id, double registered_rate, int priority){
    if (allocations.find(id) == allocations.end()) {
        return 0;
    } else {
        double allocated = allocations[id];
        total_allocated += allocated;
        return allocated;
    }
}