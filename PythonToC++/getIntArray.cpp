#include <vector>
#include <iostream>
#include <stdio.h>
#include <string>
#include <arrow/builder.h>
#include <arrow/buffer.h>
#include <plasma/client.h>
#include <fstream>
#include <stdlib.h>
#include <valarray>
#include <ctime>
#include <chrono>

using namespace plasma;

void ReadAllBytes(char* filename, std::string &line) {
    std::ifstream myfile (filename);
    if (myfile.is_open()) {
        getline (myfile,line);
    } else{std::cout << "hello" << std::endl;}
}

ObjectBuffer getObjectBuffer(std::string &bytes){
    PlasmaClient client;

    (client.Connect("/tmp/plasma", ""));
    ObjectID object_id = ObjectID::from_binary(bytes);
    ObjectBuffer object_buffer;
    (client.Get(&object_id, 1, -1, &object_buffer));
    return object_buffer;
}

int getIntArray(char* filename){
    std::string bytes;
    ReadAllBytes(filename, bytes);
    ObjectBuffer object_buffer = getObjectBuffer(bytes);
    
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    auto size = (uint64_t)((uint8_t*)object_buffer.data->size())/sizeof(int64_t);
    int64_t* data = new int64_t[size];

    std::memcpy(data,(int64_t*)object_buffer.data->data(),size*sizeof(int64_t));

    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration_cast<std::chrono::microseconds>( t2 - t1 ).count();
    std::cout << "Time (secs) for " << size << " is: " << duration/1000000 << std::endl;

}


int main() {
    char objidfile[34] = "<path_to_object_id_file>";

    getIntArray(objidfile);
}
