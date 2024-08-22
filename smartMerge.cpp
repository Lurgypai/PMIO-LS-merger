#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <algorithm>

#include <cstring>

#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>

#include "merger.h"

static void* mapFile(const std::string& filename, int size, bool trunc) {
    int flags = O_RDWR;
    if(trunc) flags |= O_CREAT | O_TRUNC;
    int file = open(filename.c_str(), flags, 0666);
    if(file < 0) {
        std::cerr << "Error opening file \"" << filename << "\"\n";
        perror("Error:");
    }
    if(trunc) ftruncate(file, size);
    void* data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);
    if(data == MAP_FAILED) {
        std::cerr << "Error mapping file \"" << filename << "\"\n";
        perror("Error:");
    }
    close(file);
    return data;
}

int main(int argc, char** argv) {

    std::string bufferFolder;

    std::string outDataFile;
    std::string outMetadataFile;

    std::string outFile;

    int maxDataSize = 131072;

    std::string activeFlag = "";
    for(int argNum = 0; argNum != argc; ++argNum) {
        std::string currArg{argv[argNum]};
        if(currArg[0] == '-') activeFlag = currArg;
        else if(activeFlag == "--bufferFolder") {
            bufferFolder = std::move(currArg);
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
        else if(activeFlag == "--maxDataSize") {
            maxDataSize = std::stoi(currArg);
        }
    }

    std::vector<std::string> metadataFiles;
    std::vector<std::string> dataFiles;

    namespace fs = std::filesystem;
    for(const auto& dirent : fs::directory_iterator(bufferFolder)) {
        std::string filename = dirent.path().filename();
        if(filename.find("metadata-log") != std::string::npos &&
                filename.find("merged") == std::string::npos) metadataFiles.push_back(dirent.path());
        else if (filename.find("data-log") != std::string::npos &&
                filename.find("merged") == std::string::npos) dataFiles.push_back(dirent.path());
    }

    std::sort(metadataFiles.begin(), metadataFiles.end());
    std::sort(dataFiles.begin(), dataFiles.end());
    
    std::cout << "bufferFolder: " << bufferFolder << '\n';
    std::cout << "metadataFiles: ";
    for (auto& file : metadataFiles) std::cout << file << ", ";
    std::cout << "\ndataFiles: ";
    for (auto& file : dataFiles) std::cout << file << ", ";
    std::cout << "\noutMetadataFile: " << outMetadataFile << "\noutDataFile: " << outDataFile << '\n';

    std::vector<m_chunk*> metadata = std::vector<m_chunk*>(metadataFiles.size(), nullptr);
    for(int i = 0; i != metadata.size(); ++i) {
        metadata[i] = static_cast<m_chunk*>(mapFile(metadataFiles[i], M_CHUNK_COUNT * sizeof(m_chunk), false));
    }

    std::vector<void*> data = std::vector<void*>(dataFiles.size(), nullptr);
    for(int i = 0; i != data.size(); ++i) {
        data[i] = mapFile(dataFiles[i], maxDataSize, false);
    }

    void* outData = mapFile(outDataFile, maxDataSize, true);

    std::vector<int> startIndices = std::vector<int>(dataFiles.size(), 0);
    std::vector<int> endIndices = std::vector<int>(dataFiles.size(), M_CHUNK_COUNT);
    
    int curEndOffset = 0;

    std::ofstream out{outFile};

    std::cout << "Merging data...\n";
    Merger merger = MergeData(metadata, data, startIndices, endIndices,
            outData, curEndOffset, maxDataSize, out);
    merger.debugLog();
    std::cout << "Merge complete.\n";

    std::cout << "Writing data...\n";
    // do final write
    std::vector<char> buffer;
    for(auto& item : merger.getItems()) {
        auto& logItem = item.getLogItems()[0];
        buffer.resize(item.getLength());
        std::memcpy(buffer.data(), static_cast<char*>(outData) + logItem.item.data_offset,
                item.getLength());
        out.seekp(logItem.item.target_offset);
        out.write(buffer.data(), item.getLength());
    }

    out.close();
    std::cout << "Write complete.\n";

    if(outMetadataFile == "") return 0;
    std::cout << "Out metadata file specified, logging merged metadata.\n";
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
