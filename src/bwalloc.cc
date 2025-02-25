#include "bwalloc.h"
#include "gconfig.h"
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

bool BandwidthAllocator::is_congested() {
    bool congested = total_registered > total_available;    
    return congested;
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////



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



MaxMinFairShareBandwidthAllocator::MaxMinFairShareBandwidthAllocator(double total_available){
    this->total_available = total_available;
    BandwidthAllocator::reset(); 
}

MaxMinFairShareBandwidthAllocator::~MaxMinFairShareBandwidthAllocator(){

}

void MaxMinFairShareBandwidthAllocator::reset(){
    BandwidthAllocator::reset(); 
    register_list.clear();
    allocations.clear();
}

void MaxMinFairShareBandwidthAllocator::register_rate(int id, double rate, int priority){
    register_list.push_back(std::make_pair(id, rate));
    total_registered += rate;
}

void MaxMinFairShareBandwidthAllocator::compute_allocations(){
    std::sort(register_list.begin(), register_list.end(), 
        [](const std::pair<int, double>& a, const std::pair<int, double>& b) -> bool {
            return a.second < b.second;
        }
    );

    bool punish = GConf::inst().punish_oversubscribed;     

    int remaining_item_count = register_list.size();
    double exceed_availablity = std::max(0.0, total_registered - total_available);

    double available = total_available;
    if (punish) {
        available -= exceed_availablity;

        double punish_threshold = total_available * GConf::inst().punish_oversubscribed_min;   
        if (available < punish_threshold) {    
            available = punish_threshold;  
        }
    }       

    for (auto& item : register_list) {
        double rate = item.second;
        double remaining_fair_share = available / remaining_item_count;
        double allocated_rate = std::min(rate, remaining_fair_share);

        allocations[item.first] = allocated_rate;

        available -= allocated_rate;
        remaining_item_count -= 1;
    }
}

double MaxMinFairShareBandwidthAllocator::get_allocated_rate(int id, double registered_rate, int priority){
    if (allocations.find(id) == allocations.end()) {
        spdlog::error("MaxMinFairShareBandwidthAllocator::get_allocated_rate() called with id that was not registered. id: {}", id);        
        return 0;
    } else {
        double allocated = allocations[id];
        total_allocated += allocated;
        return allocated;
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
        spdlog::error("PriorityQueueBandwidthAllocator::reset() called with non-empty register_queue. but the queue should have been cleared by compute_allocations(). There's something very wrong going on.");
    }
    allocations.clear();
}

void PriorityQueueBandwidthAllocator::register_rate(int id, double rate, int priority){
    total_registered += rate;
    register_queue.push(std::make_pair(-1 * priority, std::make_pair(id, rate)));
}

// void PriorityQueueBandwidthAllocator::compute_allocations(){
//     double available = total_available;

//     while (!register_queue.empty()) {
//         std::pair<int, std::pair<int, double> > item = register_queue.top();
//         register_queue.pop();
        
//         int priority = item.first;
//         int id = item.second.first;
//         double rate = item.second.second;

//         bool depleted = (rate >= available);

//         allocations[id] = std::min(rate, available);
//         available -= allocations[id];

//         if (depleted) {
//             while (!register_queue.empty()) {
//                 register_queue.pop();
//             }
//             break;
//         }
//     }
// }

void PriorityQueueBandwidthAllocator::compute_allocations(){
    double available = total_available;

    // if we have no items in the queue, we can return
    if (register_queue.empty()) {
        return;
    }

    while (available > 0 and not register_queue.empty()) {
        // we have at least one item in the queue. get the top priority. 
        int top_priority = register_queue.top().first;
        std::vector<std::pair<int, double> > top_priority_items;

        // get all the items with the top priority
        while (!register_queue.empty() && register_queue.top().first == top_priority) {
            std::pair<int, std::pair<int, double> > item = register_queue.top();
            register_queue.pop();
            top_priority_items.push_back(std::make_pair(item.second.first, item.second.second));
        }

        // get the total rate of all the items with the top priority
        double top_priority_rate_sum = 0;
        for (auto item : top_priority_items) {
            top_priority_rate_sum += item.second;
        }

        // if the total rate of all the items with the top priority is less than the available bandwidth,
        // we can allocate all the rate to the items with the top priority
        if (top_priority_rate_sum <= available) {
            for (auto item : top_priority_items) {
                allocations[item.first] = item.second;
                available -= item.second;
            }
        } else {
            // if the total rate of all the items with the top priority is more than the available bandwidth,
            // we need to allocate the bandwidth in proportion to the rate of each item. 

            double allocation_ratio = available / top_priority_rate_sum; 

            for (auto item : top_priority_items) {
                double allocated_rate = item.second * allocation_ratio; 
                allocations[item.first] = allocated_rate;
                available -= allocated_rate;
            }
        }

        // the next iteration will happen with the remaining available bandwidth, 
        // and the rest of the items in the lower priority levels.
    }

    while (!register_queue.empty()) {
        register_queue.pop();
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