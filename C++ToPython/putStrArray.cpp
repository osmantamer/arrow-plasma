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

int putStrArray(char* filename, char* fromTextFile, int size){
    /*** Generate a Str Array ***/
    std::ifstream myfile (fromTextFile);
    std::string x;
    std::string* test = new std::string[size];
    int cs = 0;
    long max_length = 0;
    while (std::getline(myfile, x)) {
        test[cs] = x;
        if (test[cs].length() > max_length ) {
            max_length = test[cs].length();
        }
        cs++;
    }

    char id_arr[20];
    getRandomId(id_arr,filename);
    std::string obj_id(id_arr);
    /*** Write Str Array to Plasma ***/
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    long total_char = 0;
    for (int i=0; i<size;i++){
        total_char += test[i].length()+1; // +1 is the space between each item
    }

    PlasmaClient client;
    (client.Connect("/tmp/plasma", ""));
    ObjectID object_id = ObjectID::from_binary(obj_id);

    int64_t data_size = total_char;;
    std::shared_ptr<Buffer> plasmaData;
    std::string metadata = "Strings";
    (client.Create(object_id, data_size, (uint8_t*) metadata.data(), metadata.size(), &plasmaData));
    // Write some data into the object.
    /* Put each string by converting into character array one by one */
    int c = 0;
    for (int i = 0; i < size; i++){
        auto tlen = test[i].length();
        char* temp = new char[tlen];
        std::copy(test[i].begin(),test[i].end(), temp);
        std::memcpy(&((char*)plasmaData->data())[c], temp, tlen*sizeof(char));
        c+=(tlen+1); // +1 to make space between each item.
    }

    (client.Seal(object_id));
    (client.Disconnect());

    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration_cast<std::chrono::microseconds>( t2 - t1 ).count();
    std::cout << "Time (secs) for " << size << " is: " << duration/1000000 << std::endl;
    return 0;
}

int main() {
    char objidfile[] = "<path_to_obj_id_file>";

    char fromTextFile[] = "<path_to_file_to_generate_input>";
    int size = 131072;
    putStrArray(objidfile,fromTextFile,size);
}
