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

int getStrArray(char* filename,char* len_maxfilename){

    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    /* find len_max */
    std::string bytes_lenmax;
    ReadAllBytes(len_maxfilename, bytes_lenmax);
    ObjectBuffer object_buffer_lenmax = getObjectBuffer(bytes_lenmax);
    int* len_max_arr = new int[1];
    std::memcpy(len_max_arr,(int*)object_buffer_lenmax.data->data(),1*sizeof(int64_t));
    std::cout << "len_max is : " << len_max_arr[0] << std::endl;
    /* find len_max */

    /* get data from plasma */
    std::string bytes;
    ReadAllBytes(filename, bytes);
    ObjectBuffer object_buffer = getObjectBuffer(bytes);

    auto size = (uint64_t)((uint8_t*)object_buffer.data->size());

    int len_max = len_max_arr[0];
    auto dataSize = size/len_max;
    std::string* data = new std::string[dataSize];
    /* data array filled */
    for (int i = 0; i<size/len_max;i++) {
        char temp[len_max];
        std::memcpy(temp, &((char *) object_buffer.data->data())[i*len_max], len_max * sizeof(char));
        data[i] = temp;
    }

    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration_cast<std::chrono::microseconds>( t2 - t1 ).count();
    std::cout << "Time (secs) for " << size << " is: " << duration/1000000 << std::endl;
}

int main() {
    char objidfile[] = "<path_to_obj_id_file>";
    char objid_lenmaxfile[] = "<path_to_obj_id_len_max_file>";

    getStrArray(objidfile,objid_lenmaxfile);
}
