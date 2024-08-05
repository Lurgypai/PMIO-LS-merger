#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstring>

#include <iostream>
#include <algorithm>
#include <fstream>
#include <unordered_map>

#include "merger.h"

Merger MergeFiles(const std::vector<std::string>& sourceLogFiles,
        const std::vector<std::string>& sourceDataFiles,
        const std::string& mergedDataFile) {
    Merger merger{2048};

    // std::cout << "MergeFIles triggered. Outputting merged to \"" << mergedDataFile << "\"\n";

    // m-log merge
    for(int currFile = 0; currFile != sourceLogFiles.size(); ++currFile) {
        auto& sourceLogFile = sourceLogFiles[currFile];
        auto& sourceDataFile = sourceDataFiles[currFile];
        // std::cout << "Opening file " << sourceLogFile << '\n';

        int fileNo = open(sourceLogFile.c_str(), O_RDWR);
        int length = lseek(fileNo, 0, SEEK_END);
        if(length < 0) {
            perror("Unable to open file");
            throw std::exception{};
        }

        void* dataAddr = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fileNo, 0);
        std::vector<m_chunk> chunks;
        chunks.resize(M_CHUNK_COUNT);
        memcpy(chunks.data(), dataAddr, chunks.size() * sizeof(m_chunk));

        for(auto& chunk : chunks) {
            if(chunk.free) continue;
            if(chunk.item_count != M_ITEM_COUNT) break; //the final chunk is the only one that could be partially filled
            for(int item_num = 0; item_num != chunk.item_count; ++item_num) {
                merger.addItem(chunk.items[item_num], sourceDataFile, chunk.req_len);
            }
            chunk.free = 1;
        }

        close(fileNo);
    }
    merger.mergeAll();

    // merger.debugLog();

    // d-log merge
    std::ofstream mergedDataOut{mergedDataFile};
    if(!mergedDataOut.good()) {
        std::cerr << "Error opening merged data output file...\n";
        throw std::exception{};
    }

    /*
     * for each item
     *  for each log item in item
     *      copy data from data file to output data file
     */
    std::unordered_map<std::string, std::ifstream> dataFiles;
    for(auto& dataFile : sourceDataFiles) {
        std::ifstream file{dataFile};
        if(!file.good()) continue;
        dataFiles.emplace(std::pair<std::string, std::ifstream>{dataFile, std::move(file)});
    }

    if(dataFiles.size() != sourceDataFiles.size()) {
        std::cerr << "Error, unable to open one or more data files\n";
        throw std::exception{};
    }

    std::vector<char> buffer;
    for(auto& item : merger.getItems()) {
        item.setDataOffset(mergedDataOut.tellp());

        auto& logItems = item.getLogItems();
        int length = item.getLength() / logItems.size();
        for(const auto& logItem : logItems) {
            auto& targetSourceFile = dataFiles[logItem.sourceDataFile];

            buffer.resize(length);
            targetSourceFile.seekg(logItem.item.data_offset);
            targetSourceFile.read(buffer.data(), length);

            // std::cout << "Copying data from " << logItem.item.data_offset << " to " << mergedDataOut.tellp() << " length " << length << '\n';
            mergedDataOut.write(buffer.data(), length);
        }

        // now that all of the items are meregd, update to the new backing item
        logItems.clear(); 
        logItems.emplace_back(TaggedItem{m_item{item.getDataOffset(), item.getBaseOffset()}, mergedDataFile});
    }
    return merger;

}

Merger::Merger(std::size_t initialBackingSize) {
    items.reserve(initialBackingSize);
}

void Merger::addItem(const m_item& item, const std::string& sourceDataFile, uint64_t length) {
    // std::cout << "Creating new merge item:\n";
    // std::cout << "\titem.target_offset: " << item.target_offset << '\n';
    // std::cout << "\tlength: " << length << '\n';
    // std::cout << "\tsourceDataFile: " << sourceDataFile << '\n';

    MergerItem newMergerItem = MergerItem{item, sourceDataFile, length};

    auto foundItemIter = std::lower_bound(items.begin(), items.end(), newMergerItem);

    // not contained
    if(foundItemIter == items.end() || *foundItemIter != newMergerItem) {
        // std::cout << "No mergeable items found, adding new\n";
        items.emplace(foundItemIter, std::move(newMergerItem));
        return;
    }

    // contained
    // std::cout << "Found mergeable item, merging\n";
    foundItemIter->merge(newMergerItem);
}

void Merger::addItemNoMerge(const m_item& item, const std::string& sourceDataFile, uint64_t length) {
    MergerItem newMergerItem = MergerItem{item, sourceDataFile, length}; 

    auto foundItemIter = std::lower_bound(items.begin(), items.end(), newMergerItem);
    items.emplace(foundItemIter, std::move(newMergerItem)); 
}

void Merger::mergeAll() {
    for(auto iter = items.begin(); iter != items.end();) {
        auto prevIter = iter++;
        // one item
        if(iter == items.end()) return;

        if(*prevIter == *iter) {
            prevIter->merge(*iter);
            items.erase(iter);
            iter = prevIter;
        }
    }
}

void Merger::clear() {
    items.clear();
}

void Merger::debugLog() const {
    std::cout << "DEBUG LOG MERGER\n";
    for(const auto& currItem : items) {
        std::cout << "Item [\n" <<
            "\ttarget offset: " << currItem.getBaseOffset() << '\n' <<
            "\tdata offset: " << currItem.getDataOffset() << '\n' <<
            "\tlen: " << currItem.getLength() << '\n';
        std::cout << "]\nSub Items [\n";
        for(const auto& subItem : currItem.getLogItems()) {
            std::cout << "\tSource Offset: " << subItem.item.data_offset << '\n';
            std::cout << "\tTarget Offset: " << subItem.item.target_offset << '\n';
            std::cout << "\tSource Filename: " << subItem.sourceDataFile << '\n';
        }
        std::cout << "]\n";
    }
}

size_t Merger::getItemCount() const {
    return items.size();
}

const std::vector<MergerItem>& Merger::getItems() const {
    return items;
}

std::vector<MergerItem>& Merger::getItems() {
    return items;
}

