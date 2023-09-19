#ifndef PROTOCOL_BUILDER_H
#define PROTOCOL_BUILDER_H

namespace psim {
Protocol* build_random_protocol(int num_comp, int machine_count); 
Protocol* load_protocol_from_file(std::string file_path); 
Protocol* pipelinize_protocol(Protocol *proto, int num_replicas, bool tight_connections = false);
Protocol* super_simple_protocol();
Protocol* simple_pipeline_protocol(int length);
Protocol* simple_protocol_v1(); 
} // namespace psim

#endif

