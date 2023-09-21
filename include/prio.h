#ifndef PRIO_H
#define PRIO_H

#include <vector>
#include <map>
#include <queue>

namespace psim {


class PriorityAllocator {
public:
    PriorityAllocator() {};
    virtual ~PriorityAllocator() {};
    
    double total_available;
    double total_registered;
    double total_allocated; 

    virtual void reset() = 0;
    virtual void register_rate(int id, double rate, int priority) = 0;
    virtual void compute_allocations() = 0;
    virtual double get_allocated_rate(int id, double registered_rate = -1, int priority = -1) = 0;
private: 
};


class FairSharePriorityAllocator : public PriorityAllocator {
public:
    FairSharePriorityAllocator(double total_available);
    virtual ~FairSharePriorityAllocator();

    void reset();
    void register_rate(int id, double rate, int priority);
    void compute_allocations();
    double get_allocated_rate(int id, double registered_rate = -1, int priority = -1);
private:
};

class FixedLevelsPriorityAllocator : public PriorityAllocator {
public:
    FixedLevelsPriorityAllocator(double total_available);
    virtual ~FixedLevelsPriorityAllocator();

    int priority_levels; 

    std::vector<double> register_map;
    std::vector<double> availability_map;
    
    void reset();
    void register_rate(int id, double rate, int priority);
    void compute_allocations();
    double get_allocated_rate(int id, double registered_rate = -1, int priority = -1);
private:
};




class PriorityQueuePriorityAllocator : public PriorityAllocator {
public:
    PriorityQueuePriorityAllocator(double total_available);
    virtual ~PriorityQueuePriorityAllocator();

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
#endif // PRIO_H