#include <iostream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <thread>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "merger.h"
#include "mergeThread.h"

static bool keepMerging = true;
static Merger MasterMerger{2048};
static std::thread MergeThread_;
static bool paused = false;
static int curOutOffset = 0;

static void StartMergeThread_(const std::string targetFilename,
        std::vector<std::string> metadataFileNames,
        std::vector<std::string> dataFileNames,
        const std::string outDataFilename,
        int maxChunkInUseCount, int maxMasterItemCount, int stripeSize);

void MergeThread::StartMergeThread(std::string targetFilename,
        std::vector<std::string> metadataFileNames,
        std::vector<std::string> dataFileNames,
        std::string outDataFilename,
        int maxChunkInUseCount, int maxMasterItemCount, int stripeSize) {

    keepMerging = true;

    MergeThread_ = std::thread(StartMergeThread_, std::move(targetFilename),
            std::move(metadataFileNames), std::move(dataFileNames),
            std::move(outDataFilename),
            maxChunkInUseCount, maxMasterItemCount, stripeSize);
            
}

static void WriteFromMasterMerger(void* inData, std::ofstream& outputFile) {
    std::cout << "MergeThread.cpp: Triggered merge from master merger, writing from address " << inData << '\n';

    std::vector<char> buffer;
    for(auto& item : MasterMerger.getItems()) {
        // there should only be one
        auto& logItem = item.getLogItems()[0];

        buffer.resize(item.getLength()); 
        std::memcpy(buffer.data(), static_cast<char*>(inData) + logItem.item.data_offset, item.getLength());

        outputFile.seekp(logItem.item.target_offset);
        outputFile.write(buffer.data(), item.getLength());

        /* DEBUG */
        /*
        std::cout << "\tCopied data from " << inData << " + " << logItem.item.data_offset <<
                        " to " << logItem.item.target_offset <<
                        ", length " << item.getLength();
        int intCount = item.getLength() / 4;
        if(intCount == 1) std::cout << ", data: " << *reinterpret_cast<int*>(buffer.data()) << '\n';
        else {
            int* data = reinterpret_cast<int*>(buffer.data());
            std::cout << ", data: ";
            for(int i = 0; i != 3; ++i) {
                std::cout << data[i] << ", ";
            }
            std::cout << "... ";
            for(int i = intCount - 3; i != intCount; ++i) {
                std::cout << data[i] << ", ";
            }
            std::cout << '\n';
        }
        */
        /* END DEBUG */
    }
    MasterMerger.clear();

    curOutOffset = 0;
};

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

static void StartMergeThread_(const std::string targetFilename,
        std::vector<std::string> metadataFileNames,
        std::vector<std::string> dataFileNames,
        const std::string outDataFilename,
        int maxChunkInUseCount, int maxMasterItemCount, int stripeSize) {

    std::sort(metadataFileNames.begin(), metadataFileNames.end());
    std::sort(dataFileNames.begin(), dataFileNames.end());

    std::vector<m_chunk*> metadata = std::vector<m_chunk*>(metadataFileNames.size(), nullptr);
    for(int i = 0; i != metadata.size(); ++i) {
        metadata[i] = static_cast<m_chunk*>(mapFile(metadataFileNames[i]));
    }

    std::vector<void*> data = std::vector<void*>(dataFileNames.size(), nullptr);
    for(int i = 0; i != data.size(); ++i) {
        data[i] = mapFile(dataFileNames[i]);
    }

    void* outData = mapFile(outDataFilename);

    std::vector<int> currChunkStartIndices = std::vector<int>(metadata.size(), 0);
    std::vector<int> currChunkEndIndices = std::vector<int>(metadata.size(), 0);

    std::ofstream outFile{targetFilename};
    if(!outFile.good()) {
        std::cerr << "Unable to open out file!\n";
        return;
    }

    while(keepMerging) {

        
        // TODO actually use a cv
        if(paused) continue;

        // check metadata extent for each file
        // flag files with too much
        // merge logs that are too large
        // add returned mergers merged items to the second layer merger
        // if second layer full, merge and destage

        int metadataFileNum = 0;
        bool doMerge = false;
        for(auto& chunks : metadata) {
            // move end
            auto* curEndChunk = &chunks[currChunkEndIndices[metadataFileNum]];

            while(!curEndChunk->free && curEndChunk->item_count == M_ITEM_COUNT) {
                currChunkEndIndices[metadataFileNum] = curEndChunk->next_chunk;
                curEndChunk = &chunks[curEndChunk->next_chunk];
            }
            // check if to many chunks in use
            auto startIndex = currChunkStartIndices[metadataFileNum];
            auto endIndex = currChunkEndIndices[metadataFileNum];
            int usedChunks = endIndex - startIndex;
            if(usedChunks < 0) usedChunks = (M_CHUNK_COUNT - startIndex) + endIndex;

            if(usedChunks > maxChunkInUseCount) {
                // std::cout << "mergeThread.cpp: File number " << metadataFileNum << " \"" << metadataFileNames[metadataFileNum] << "\" has triggered a merge.\n";
                doMerge = true;
                break;
            }
            
            ++metadataFileNum;
        }

        if(!doMerge) continue;

        // how do we update the start chunk pointer while merging?
        //  add cleaning to the merge process (freeing of used)
        //  only merge full slots
        //  update the head in this code after merge completion

        std::cout << "mergeThread.cpp: Merging triggered within loop.\n";
        Merger subMerger = MergeData(metadata, data, currChunkStartIndices, currChunkEndIndices,
                outData, curOutOffset);
        std::cout << "mergeThread.cpp: Merging complete.\n";
        // subMerger.debugLog();

        for(auto& item : subMerger.getItems()) {
            // possibly don't merge the items here, just insert them
            // unsure if we want to merge again before outputing
            MasterMerger.addItemNoMerge(m_item{item.getDataOffset(), item.getBaseOffset()},
                    outData, item.getLength());
        }

        // update head
        metadataFileNum = 0;
        for(const auto& chunks : metadata) {
            auto* curStartChunk = &chunks[currChunkStartIndices[metadataFileNum]]; 
            while(!chunks[curStartChunk->next_chunk].free) {
                ++currChunkStartIndices[metadataFileNum];
                currChunkStartIndices[metadataFileNum] %= M_CHUNK_COUNT;
                curStartChunk = &chunks[currChunkStartIndices[metadataFileNum]];
            }
            ++metadataFileNum;
        }

        // check if we're too full
        // if(MasterMerger.getItemCount() <= maxMasterItemCount) continue;
        if(curOutOffset < 131072 ) continue;
        // if so commit IO's

        WriteFromMasterMerger(outData, outFile);
    }

    std::cout << "mergeThread.cpp: Comitting final flush merge.\n";
    std::vector<int> startIndices = std::vector<int>(dataFileNames.size(), 0);
    std::vector<int> endIndices = std::vector<int>(dataFileNames.size(), M_CHUNK_COUNT);

    Merger subMerger = MergeData(metadata, data, startIndices, endIndices,
            outData, curOutOffset);
    std::cout << "mergeThread.cpp: Merging complete.\n";
    // subMerger.debugLog();

    for(auto& item : subMerger.getItems()) {
        MasterMerger.addItemNoMerge(m_item{item.getDataOffset(), item.getBaseOffset()},
                outData, item.getLength());
    }

    WriteFromMasterMerger(outData, outFile);

    outFile.close();
    for(const auto& chunks : metadata) munmap(chunks, M_CHUNK_COUNT * sizeof(m_chunk));
}
void MergeThread::StopMergeThread() {
    keepMerging = false;    
    MergeThread_.join();
}

void MergeThread::PauseMergeThread() {
    paused = true;
}

void MergeThread::UnpauseMergeThread() {
    paused = false;
}

extern "C" void start_merge_thread(const char* fileName,
        char** metadataFileNames,
        char** dataFileNames,
        int fileCount,
        const char* outDataFileName,
        int maxChunkInUseCount, int maxMasterItemCount, int stripeSize) {
    std::vector<std::string> metadataNameVec, dataNameVec;
    for(int i = 0; i != fileCount; ++i) {
        metadataNameVec.emplace_back(metadataFileNames[i]);
        dataNameVec.emplace_back(dataFileNames[i]);
    }

    MergeThread::StartMergeThread(fileName, metadataNameVec, dataNameVec,
            outDataFileName, maxChunkInUseCount, maxMasterItemCount, stripeSize);
}

extern "C" void stop_merge_thread() {
    MergeThread::StopMergeThread();
}

extern "C" void pause_merge_thread() {
    MergeThread::PauseMergeThread();
}

extern "C" void unpause_merge_thread() {
    MergeThread::UnpauseMergeThread();
}
