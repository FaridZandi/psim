#ifndef OPTIONS_H
#define OPTIONS_H

#include <boost/program_options.hpp>
namespace po = boost::program_options;

namespace psim {
    void setup_logger(po::variables_map vm); 

    po::variables_map parse_arguments(int argc, char** argv); 

    void process_arguments(po::variables_map vm); 

    void log_config();
} // namespace psim

#endif