#include <iostream>
#include <string>
#include <fstream>
#include <vector>

#include "mChunk.h"

int main(int argc, char** argv) {
    std::string logFileName{argv[1]}; 
    std::ifstream logFile{logFileName};
    if(!logFile.good()) {
        std::cerr << "Unable to open file \"" << logFileName << "\"\n";
        return 1;
    }
    
    std::vector<m_chunk> chunks = std::vector(M_CHUNK_COUNT, m_chunk{});
    logFile.read(reinterpret_cast<char*>(chunks.data()), chunks.size() * sizeof(m_chunk)); 

    for(auto& chunk : chunks) {
        std::cout << "----- CHUNK -----\n";
        std::cout << "chunk.free: " << chunk.free << '\n';
        std::cout << "chunk.item_count: " << chunk.item_count << '\n';
        std::cout << "chunk.req_len: " << chunk.req_len << '\n';
        std::cout << "chunk.next_chunk: " << chunk.next_chunk << '\n';
        for(int curItemI = 0; curItemI != chunk.item_count; ++curItemI) {
            auto& item = chunk.items[curItemI];
            std::cout << "\tITEM\n";
            std::cout << "\titem.target_offset: " << item.target_offset << '\n';
            std::cout << "\titem.data_offset: " << item.data_offset << '\n';
        }
    }


    return 0;
}
