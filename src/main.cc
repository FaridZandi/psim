#include "matplotlibcpp.h"
#include "psim.h"
#include "protocol.h"
#include <iostream>


int main() {
    psim::Protocol* proto = new psim::Protocol();

    psim::PSim* psim = new psim::PSim(proto);
    
    psim::PTask* new_havij_task;
    new_havij_task = proto->create_task(psim::PTaskType::COMPUTE, 10);
    psim::PComp* new_compute_task = (psim::PComp*) new_havij_task;

    new_compute_task->size = 200;

    proto->build_dependency_graph();

    double psim_time = psim->simulate();

    std::cout << "havij time:" << psim_time << std::endl;

    return 0;
}