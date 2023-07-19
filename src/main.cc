#include "matplotlibcpp.h"
#include "psim.h"
#include "protocol.h"
#include <iostream>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/vector.hpp> 



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

    serialize_date();

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