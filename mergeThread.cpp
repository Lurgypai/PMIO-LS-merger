#include <iostream>
#include <unordered_map>
#include <fstream>
#include <algorithm>

#include <vector>

#include "merger.h"
#include "mergeThread.h"

static bool keepMerging = true;
static Merger MasterMerger{2048};

static void WriteFromMasterMerger(std::fstream& inputFile, std::ofstream& outputFile) {
    std::vector<char> buffer;
    for(auto& item : MasterMerger.getItems()) {
        // there should only be one
        auto& logItem = item.getLogItems()[0];

        buffer.resize(item.getLength()); 
        inputFile.seekg(logItem.item.data_offset);
        inputFile.read(buffer.data(), item.getLength());
        
        outputFile.seekp(logItem.item.target_offset);
        outputFile.write(buffer.data(), item.getLength());
    }
    MasterMerger.clear();
};


void MergeThread::StartMergeThread(const std::string targetFilename,
        std::vector<std::string> metadataFileNames,
        std::vector<std::string> dataFileNames,
        const std::string outDataFilename,
        int maxChunkInUseCount, int maxMasterItemCount, int stripeSize) {

    std::sort(metadataFileNames.begin(), metadataFileNames.end());
    std::sort(dataFileNames.begin(), dataFileNames.end());

    std::unordered_map<std::string, std::ifstream> metadataFiles;
    std::unordered_map<std::string, std::ifstream> dataFiles;
    for(auto& metadataFilename : metadataFileNames) {
        std::ifstream file{metadataFilename};
        if(!file.good()) {
            std::cerr << "Unable to open metadata file \"" << metadataFilename << "\"\n";
            return;
        }
        metadataFiles.emplace(metadataFilename, std::move(file));
    }

    for(auto& dataFilename : dataFileNames) {
        std::ifstream file{dataFilename};
        if(!file.good()) {
            std::cerr << "Unable to open data file \"" << dataFilename << "\"\n";
            return;
        }
        dataFiles.emplace(dataFilename, std::move(file));
    }

    std::fstream outDataFile{outDataFilename, std::ios::in | std::ios::out | std::ios::trunc};
    if(!outDataFile.good()) {
        std::cerr << "Unable to open out data file \"" << outDataFilename << "\"\n";
        return;
    }

    std::vector<m_chunk> chunkBuffer = std::vector<m_chunk>(M_CHUNK_COUNT, m_chunk{});

    std::vector<int> currChunkStartIndices = std::vector<int>(metadataFiles.size(), 0);
    std::vector<int> currChunkEndIndices = std::vector<int>(metadataFiles.size(), 0);

    while(keepMerging) {
        // check metadata extent for each file
        // flag files with too much
        // merge logs that are too large
        // add returned mergers merged items to the second layer merger
        // if second layer full, merge and destage

        int metadataFileNum = 0;
        bool doMerge = false;
        for(auto& metadataFile : metadataFiles) {
            // move end
            metadataFile.second.read(reinterpret_cast<char*>(chunkBuffer.data()),
                    chunkBuffer.size() * sizeof(m_chunk));
            auto* curEndChunk = &chunkBuffer[currChunkEndIndices[metadataFileNum]];
            while(!chunkBuffer[curEndChunk->next_chunk].free) {
                ++currChunkEndIndices[metadataFileNum];
                currChunkEndIndices[metadataFileNum] %= M_CHUNK_COUNT;
                curEndChunk = &chunkBuffer[currChunkEndIndices[metadataFileNum]];
            }
            // check if to many chunks in use
            auto startIndex = currChunkStartIndices[metadataFileNum];
            auto endIndex = currChunkEndIndices[metadataFileNum];
            int usedChunks = endIndex - startIndex;
            if(usedChunks < 0) usedChunks = (M_CHUNK_COUNT - startIndex) + endIndex;
            std::cout << "current used chunks " << usedChunks << '\n';

            if(usedChunks > maxChunkInUseCount) {
                doMerge = true;
                break;
            }
            

            ++metadataFileNum;
        }

        if(!doMerge) continue;
        std::cout << "Merging thread triggered merging\n";

        // how do we update the start chunk pointer while merging?
        //  add cleaning to the merge process (freeing of used)
        //  only merge full slots
        //  update the head in this code after merge completion

        Merger subMerger = MergeFiles(metadataFileNames, dataFileNames, outDataFilename);

        // std::cout << "Logging subMerger from triggered merge\n";
        // subMerger.debugLog();

        for(auto& item : subMerger.getItems()) {
            // possibly don't merge the items here, just insert them
            // unsure if we want to merge again before outputing
            MasterMerger.addItemNoMerge(m_item{item.getDataOffset(), item.getBaseOffset()},
                    outDataFilename, item.getLength());
        }

        // update head
        metadataFileNum = 0;
        for(auto& metadataFile : metadataFiles) {
            metadataFile.second.read(reinterpret_cast<char*>(chunkBuffer.data()),
                    chunkBuffer.size() * sizeof(m_chunk));
            auto* curStartChunk = &chunkBuffer[currChunkStartIndices[metadataFileNum]]; 
            while(!chunkBuffer[curStartChunk->next_chunk].free) {
                ++currChunkStartIndices[metadataFileNum];
                currChunkStartIndices[metadataFileNum] %= M_CHUNK_COUNT;
                curStartChunk = &chunkBuffer[currChunkStartIndices[metadataFileNum]];
            }
            ++metadataFileNum;
        }

        // check if we're too full
        if(MasterMerger.getItemCount() <= maxMasterItemCount) continue;
        // if so commit IO's
        std::ofstream outFile{targetFilename};
        if(!outFile.good()) {
            std::cerr << "Unable to open out file!\n";
            return;
        }

        // std::cout << "Logging MasterMerger from sub merge\n";
        // MasterMerger.debugLog();
        WriteFromMasterMerger(outDataFile, outFile);

        outFile.close();

        /*
        //find stripe aligned IO
        auto& items = MasterMerger.getItems();
        int offset = items[0].getBaseOffset();
        int remain = offset % stripeSize;
        if(remain != 0) offset += stripeSize - remain;
        
        
        for(auto iter = items.begin(); iter != items.end();) {
            if(iter->getLength() < stripeSize) continue;
            // insert before if necessary
            if(offset > iter->getBaseOffset()) {
                iter = items.insert(iter, MergerItem{m_item{iter->getDataOffset(), iter->getBaseOffset()}, outDataFilename, iter->getBaseOffset() - offset});
                ++iter;
            }
            // set to the second half
            auto newBegin = offset + stripeSize;
            auto end = iter->getBaseOffset() + iter->getLength();
            iter->setLength(end - newBegin);

            // if empty
            if(iter->getLength() == 0) {
                iter = items.erase(iter);
                continue;
            }

            // finish setting if not empty 
            // 
            int translation = newBegin - iter->getBaseOffset();
            iter->setBaseOffset(newBegin);
            iter->setDataOffset(iter->getDataOffset() + translation);
            ++iter;
        }
        */
    }


    std::cout << "Comitting flush merge\n";
    Merger subMerger = MergeFiles(metadataFileNames, dataFileNames, outDataFilename);
    // std::cout << "logging subMerger from flush merge\n";
    // subMerger.debugLog();

    for(auto& item : subMerger.getItems()) {
        MasterMerger.addItemNoMerge(m_item{item.getDataOffset(), item.getBaseOffset()},
                outDataFilename, item.getLength());
    }

    std::ofstream outFile{targetFilename};
    if(!outFile.good()) {
        std::cerr << "Unable to open out file!\n";
        return;
    }

    // std::cout << "Logging MasterMerger before final merge\n";
    // MasterMerger.debugLog();
    WriteFromMasterMerger(outDataFile, outFile);

    outFile.close();
    outDataFile.close();
    for(auto& pair : dataFiles) pair.second.close();
    for(auto& pair : metadataFiles) pair.second.close();
}
void MergeThread::StopMergeThread() {
    keepMerging = false;    
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
        dataNameVec.emplace_back(metadataFileNames[i]);
    }

    MergeThread::StartMergeThread(fileName, metadataNameVec, dataNameVec,
            outDataFileName, maxChunkInUseCOunt, maxMasterItemCount, stripeSize);
}

extern "C" void stop_merge_thread() {
    MergeThread::StopMergeThread();
}
