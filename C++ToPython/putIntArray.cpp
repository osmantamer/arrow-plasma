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

void genRandom(char* s, int len) { // Random string generator function.
    char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
    std::srand(std::time(NULL));
    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand()  % (sizeof(alphanum) - 1)];
    }
}

void getRandomId(char* putid, char* filename){
    genRandom(putid,20);
    std::ofstream myfile (filename);
    if (myfile.is_open()) {
        myfile << putid;
        myfile.close();
    }
}

int getIntData(char* filename, int* data){
    std::ifstream inFile;
    inFile.open(filename);
    int x;
    int c = 0;
    while (inFile >> x) {
        data[c] = x;
        c++;
    }
    inFile.close();
    return 0;
}

int putIntArray(char* filename, char* fromTextFile, int size){
    /*** Generate a Int Array ***/
    int data_size = size*sizeof(int);
    int* test = new int[size];
    getIntData(fromTextFile,test);
    char id_arr[20];
    getRandomId(id_arr,filename);
    std::string obj_id(id_arr);

    /*** Write Int Array to Plasma ***/
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    PlasmaClient client;
    client.Connect("/tmp/plasma", "");
    ObjectID object_id = ObjectID::from_binary(obj_id);

    std::shared_ptr<Buffer> data;

    std::string metadata = "Integers";
    client.Create(object_id, data_size, (uint8_t*) metadata.data(), metadata.size(), &data);

    std::memcpy((int*)data->data(),test,size*sizeof(int));

    (client.Seal(object_id));
    (client.Disconnect());

    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration_cast<std::chrono::microseconds>( t2 - t1 ).count();
    std::cout << "Time (secs) for " << size << " is: " << duration/1000000 << std::endl;
    return 0;
}



int main() {
    char objidfile[] = "<path_to_object_id_file>";

    char fromTextFile[] = "<path_to_file_to_create_input>";
    int size = 131072;

    putIntArray(objidfile,fromTextFile,size);
}
