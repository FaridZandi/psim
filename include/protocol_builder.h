#ifndef PROTOCOL_BUILDER_H
#define PROTOCOL_BUILDER_H

namespace psim {
Protocol* build_random_protocol(int num_comp, int machine_count); 
Protocol* load_protocol_from_file(std::string file_path); 
Protocol* pipelinize_protocol(Protocol *proto, int num_replicas, bool tight_connections = false);
Protocol* super_simple_protocol();
Protocol* simple_pipeline_protocol(int length);
Protocol* ring_allreduce(int num_replicas, double comm_size, double aggregate_time); 
Protocol* build_all_to_all(int num_replicas, double comm_size, int chunk_count); 
} // namespace psim

#endif

