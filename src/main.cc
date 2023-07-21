#include "psim.h"
#include "protocol.h"
#include <iostream>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/vector.hpp> 


using namespace psim;


class Test {
public:
    
    Test(int a, int b) : a(a), b(b) {
        std::cout << "Test created with a: " << a << " b: " << b << std::endl;
    }

    void print(){
        std::cout << "a: " << a << " b: " << b << std::endl; 
    }

    int a; 
    int b; 

    // boost serialization
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version){
        ar & a;
        ar & b;
    }


};

void serialize_date();

int main(int argc, char** argv) {
    srand(time(NULL));
    
    std::string file_name = "";

    if (argc < 2) {
        std::cout << "Please provide a protocol file name" << std::endl;
        return 1;
    } else {
        file_name = argv[1];
    }

    // Protocol* proto = Protocol::build_random_protocol(1600, 16);
    Protocol* base_proto = Protocol::load_protocol_from_file(file_name);
    // Protocol* base_proto = Protocol::super_simple_protocol();
    // Protocol* base_proto = Protocol::super_simple_protocol();

    base_proto->build_dependency_graph();

    Protocol* proto = Protocol::pipelinize_protocol(base_proto, 2, true);
    // Protocol* proto = base_proto->make_copy(true);
    // Protocol* proto = base_proto; 

    // std::string path = "logs/protocol_log.txt";
    // std::ofstream simulation_log;
    // simulation_log.open(path);
    // proto->export_graph(simulation_log);
    // simulation_log.close();
    proto->export_dot("protocol");

    PSim* psim = new PSim(proto);
    double psim_time = psim->simulate();
    std::cout << "havij time:" << psim_time << std::endl;

    return 0;
}



void serialize_date(){
    Test data_to_dump(1, 2);
    std::ofstream ofs("filename");
    boost::archive::text_oarchive oa(ofs);  
    oa << data_to_dump;
    ofs.close();


    Test data_to_load(0, 0);
    std::ifstream ifs("filename");
    boost::archive::text_iarchive ia(ifs);
    ia >> data_to_load;
    ifs.close();

    std::cout << "data_to_load: " << std::endl;
    data_to_load.print();
}