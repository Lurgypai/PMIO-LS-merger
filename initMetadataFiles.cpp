#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include "mChunk.h"

int main(int argc, char** argv) {
    if(argc == 1) return 0;

    std::vector<std::string> filenames;

    std::cout << "Getting file names...\n";
    for(int arg = 1; arg != argc; ++arg) {
        filenames.emplace_back(argv[arg]);
    }

    std::cout << "Preparing buffer...\n";
    std::vector<m_chunk> chunkBuffer = std::vector<m_chunk>(M_CHUNK_COUNT, m_chunk{0, 1, 0, 0, 0, 0, {}});
    for(int i = 0; i != M_CHUNK_COUNT; ++i) {
        chunkBuffer[i].next_chunk = i + 1;
    }
    chunkBuffer[M_CHUNK_COUNT - 1].next_chunk = 0;

    std::cout << "Writing base data to metadata files\n";
    for(const auto& filename : filenames) {
        std::ofstream file{filename};
        if(!file.good()) {
            std::cout << "Error opening file \"" << filename << "\"\n";
            return 1;
        }
        file.write(reinterpret_cast<char*>(chunkBuffer.data()), chunkBuffer.size() * sizeof(m_chunk));
    }

    std::cout << "Metadata files initialized\n";
    return 0;
}
