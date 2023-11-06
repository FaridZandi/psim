#ifndef OPTIONS_H
#define OPTIONS_H

#include <boost/program_options.hpp>
namespace po = boost::program_options;

namespace psim {
    void setup_logger(bool recreate_dir = true); 

    po::variables_map parse_arguments(int argc, char** argv); 

    void process_arguments(po::variables_map vm); 

    void log_config();

    void change_log_path(std::string output_dir, 
                         std::string log_file_name, 
                         bool recreate_dir = false);
} // namespace psim

#endif