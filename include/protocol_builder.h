#ifndef PROTOCOL_BUILDER_H
#define PROTOCOL_BUILDER_H

namespace psim {


struct ProtocolFlowSpec{
    int job_id;
    int per_job_task_id; 
    int iteration; 

    bool operator<(const ProtocolFlowSpec& other) const {
        if (job_id < other.job_id) {
            return true;
        } else if (job_id == other.job_id) {
            if (per_job_task_id < other.per_job_task_id) {
                return true;
            } else if (per_job_task_id == other.per_job_task_id) {
                if (iteration < other.iteration) {
                    return true;
                }
            }
        }
        return false;
    }   
}; 

struct RoutingSpec {
    int spine_count;    
    std::vector<std::pair<int, double>> spine_rates;    

    // init 
    RoutingSpec() {
        spine_count = 0; 
    }
}; 

Protocol* build_random_protocol(int num_comp, int machine_count); 
Protocol* load_protocol_from_file(std::string file_path); 
Protocol* pipelinize_protocol(Protocol *proto, int num_replicas, bool tight_connections = false);
Protocol* super_simple_protocol();
Protocol* simple_pipeline_protocol(int length);
Protocol* ring_allreduce(int num_replicas, double comm_size, double aggregate_time); 
Protocol* build_all_to_all(int num_replicas, double comm_size, int chunk_count); 
Protocol* build_periodic_data_parallelism(); 
Protocol* build_periodic_simple();
Protocol* build_nethint_test(); 
} // namespace psim

#endif

