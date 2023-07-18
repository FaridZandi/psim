#ifndef PSIM_H
#define PSIM_H

#include <vector>
#include <map>
#include <cmath>
#include <queue>
#include <limits>
#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <fstream>
#include "constants.h"

namespace psim {

class Protocol;
class PSim;
class Network;
class FatTreeNetwork;
class Machine;
class Bottleneck;
class Flow;
class PTask;
class PComp;
class EmptyTask;

class PSim {
public:
    
    PSim();

    PSim(Protocol *protocol);
    
    virtual ~PSim();

    virtual double simulate();

    Protocol *protocol;

private: 
    double timer;
    double step_size;

    Network *network;

    std::vector<Flow *> flows; 
    std::vector<PComp *> compute_tasks;

    std::vector<Flow *> finished_flows;
    std::vector<PComp *> finished_compute_tasks;
    
    void start_next_tasks(PTask *task);
    void start_task(PTask *task);
    double make_progress_on_flows(std::vector<Flow*> & step_finished_flows); 

}; 

} // namespace havij
#endif // PSIM_H