#ifndef BWALLOC_H
#define BWALLOC_H

#include <vector>
#include <map>
#include <queue>

namespace psim {


class BandwidthAllocator {
public:
    BandwidthAllocator() {};
    virtual ~BandwidthAllocator() {};
    
    double total_available;
    double total_registered;
    double total_allocated; 
    double utilized_bandwidth;

    virtual void reset();
    virtual void register_rate(int id, double rate, int priority) = 0;
    virtual void compute_allocations() = 0;
    virtual double get_allocated_rate(int id, double registered_rate = -1, int priority = -1) = 0;
    
    void register_utilization(double utilization);
    bool is_congested(); 
    
private: 
};


class FairShareBandwidthAllocator : public BandwidthAllocator {
public:
    FairShareBandwidthAllocator(double total_available);
    virtual ~FairShareBandwidthAllocator();


    void reset();
    void register_rate(int id, double rate, int priority);
    void compute_allocations();
    double get_allocated_rate(int id, double registered_rate = -1, int priority = -1);
private:
};


class MaxMinFairShareBandwidthAllocator : public BandwidthAllocator {
public:
    MaxMinFairShareBandwidthAllocator(double total_available);
    virtual ~MaxMinFairShareBandwidthAllocator();

    std::vector<std::pair<int, double> > register_list;
    std::map<int, double> allocations; 
    
    void reset();
    void register_rate(int id, double rate, int priority);
    void compute_allocations();
    double get_allocated_rate(int id, double registered_rate = -1, int priority = -1);

private:
};


class FixedLevelsBandwidthAllocator : public BandwidthAllocator {
public:
    FixedLevelsBandwidthAllocator(double total_available);
    virtual ~FixedLevelsBandwidthAllocator();

    int priority_levels; 

    std::vector<double> register_map;
    std::vector<double> availability_map;
    
    void reset();
    void register_rate(int id, double rate, int priority);
    void compute_allocations();
    double get_allocated_rate(int id, double registered_rate = -1, int priority = -1);

private:
};




class PriorityQueueBandwidthAllocator : public BandwidthAllocator {
public:
    PriorityQueueBandwidthAllocator(double total_available);
    virtual ~PriorityQueueBandwidthAllocator();

    // a priority queue, items added with <priority, <id, rate> >
    std::priority_queue<std::pair<int, std::pair<int, double> > > register_queue; 
    std::map<int, double> allocations; 

    void reset();
    void register_rate(int id, double rate, int priority);
    void compute_allocations();
    double get_allocated_rate(int id, double registered_rate = -1, int priority = -1);

private:
};

} // namespace psim
#endif // BWALLOC_H