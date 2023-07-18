#include <iostream>
#include "psim.h"


int main() {
  std::cout << "Hello, world!" << std::endl;

  havij::HavijSimulator* havij_simulator = new havij::HavijSimulator();

  havij::Protocol* havij_protocol = havij_simulator->protocol;
  
  havij::HavijTask* new_havij_task;
  new_havij_task = havij_protocol->create_task(havij::HavijTaskType::COMPUTE, 10);
  havij::ComputeTask* new_compute_task = (havij::ComputeTask*)new_havij_task;

  new_compute_task->size = 100;

  havij_protocol->build_dependency_graph();

  double havij_sim_time = havij_simulator->simulate();

  std::cout << " havij time:" << havij_sim_time << std::endl;

  return 0;
}