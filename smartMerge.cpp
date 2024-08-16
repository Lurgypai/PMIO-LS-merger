#include <string>
#include <vector>
#include <fstream>
#include <iostream>

#include <cstring>

#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>

#include "merger.h"

static void* mapFile(const std::string& filename) {
    int file = open(filename.c_str(), O_RDWR, 0666);
    if(file < 0) {
        std::cerr << "Error opening file \"" << filename << "\"\n";
        perror("Error:");
    }
    void* data = mmap(NULL, M_CHUNK_COUNT * sizeof(m_chunk), PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);
    if(data == MAP_FAILED) {
        std::cerr << "Error mapping file \"" << filename << "\"\n";
        perror("Error:");
    }
    close(file);
    return data;
}

int main(int argc, char** argv) {
    std::vector<std::string> dataFiles;
    std::vector<std::string> metadataFiles;

    std::string outDataFile;
    std::string outMetadataFile;

    std::string outFile;

    std::string activeFlag = "";
    for(int argNum = 0; argNum != argc; ++argNum) {
        std::string currArg{argv[argNum]};
        if(currArg[0] == '-') activeFlag = currArg;
        else if(activeFlag == "--dataFiles") {
            dataFiles.emplace_back(std::move(currArg));
        }
        else if(activeFlag == "--metadataFiles") {
            metadataFiles.emplace_back(std::move(currArg));
        }
        else if (activeFlag == "--outDataFile") {
            outDataFile = std::move(currArg);
        }
        else if (activeFlag == "--outMetadataFile") {
            outMetadataFile = std::move(currArg);
        }
        else if (activeFlag == "--outFile") {
            outFile = std::move(currArg);
        }
    }
    
    /*
    std::cout << "metadataFiles: ";
    for (auto& file : metadataFiles) std::cout << file << ", ";
    std::cout << "\ndataFiles: ";
    for (auto& file : dataFiles) std::cout << file << ", ";
    std::cout << "\noutMetadataFile: " << outMetadataFile << "\noutDataFile: " << outDataFile << '\n';
    */

    std::vector<m_chunk*> metadata = std::vector<m_chunk*>(metadataFiles.size(), nullptr);
    for(int i = 0; i != metadata.size(); ++i) {
        metadata[i] = static_cast<m_chunk*>(mapFile(metadataFiles[i]));
    }

    std::vector<void*> data = std::vector<void*>(dataFiles.size(), nullptr);
    for(int i = 0; i != data.size(); ++i) {
        data[i] = mapFile(dataFiles[i]);
    }

    void* outData = mapFile(outDataFile);

    std::vector<int> startIndices = std::vector<int>(dataFiles.size(), 0);
    std::vector<int> endIndices = std::vector<int>(dataFiles.size(), M_CHUNK_COUNT);
    
    int curEndOffset = 0;

    std::ofstream out{outFile};

    Merger merger = MergeData(metadata, data, startIndices, endIndices,
            outData, curEndOffset, 131072, out);

    // write out metadata (not done because normally this stays in memory)
    std::vector<m_chunk> outChunks = std::vector<m_chunk>(M_CHUNK_COUNT, m_chunk{});
    for(int curChunk = 0; curChunk != outChunks.size(); ++curChunk) {
        auto& chunk = outChunks[curChunk];
        chunk.free = 1;
        chunk.item_count = 0;
        chunk.next_chunk = curChunk+1;
    }
    outChunks[M_ITEM_COUNT - 1].next_chunk = 0;
    int curChunkIndex = 0;

    for(auto& item : merger.getItems()) {
        auto& curChunk = outChunks[curChunkIndex];
        if(curChunk.free) {
            curChunk.free = 0;
            curChunk.item_count = 0;
            curChunk.req_len = item.getLength();
        }

        auto& curItem = curChunk.items[curChunk.item_count];
        curItem.data_offset = item.getDataOffset();
        curItem.target_offset = item.getBaseOffset();

        ++curChunk.item_count;
        if(curChunk.item_count == M_ITEM_COUNT) curChunkIndex = curChunk.next_chunk;
    } 

    std::ofstream mergedMetadataOut{outMetadataFile};
    if(!mergedMetadataOut.good()) {
        std::cerr << "Unable to open to output metadata file\n";
        return 1;
    }
    mergedMetadataOut.write(reinterpret_cast<char*>(outChunks.data()), outChunks.size() * sizeof(m_chunk));
    return 0;
}
