#include <fcntl.h>
#include <fstream>
#include <sys/mman.h>
#include <unistd.h>

#include <cstring>

#include <iostream>
#include <algorithm>
#include <vector>

#include "merger.h"

Merger MergeData(const std::vector<m_chunk*>& sourceMetadata,
        const std::vector<void*>& sourceData,
        const std::vector<int>& leadingChunks, const std::vector<int>& endChunks,
        void* outData, int& curOutOffset, int maxSize,
        std::ofstream& fileOut) {

    Merger merger{2048};

    // m-log merge
    for(int i = 0; i != sourceMetadata.size(); ++i) {
        auto& chunks = sourceMetadata[i];
        auto& data = sourceData[i];
        auto leadingChunk = leadingChunks[i];
        auto endChunk = endChunks[i];
        auto chunkCount = endChunk - leadingChunk;
        if(chunkCount < 0) chunkCount = (M_CHUNK_COUNT - leadingChunk) + endChunk;

        /* DEBUG */
        // std::cout << "merger.cpp: Adding items from chunks " << leadingChunk << " to " << endChunk << ", " << chunkCount << " chunks.\n";
        // int actualChunkCount = 0;
        /* END DEBUG */

        for(int j = 0; j != chunkCount; ++j) {
            auto& chunk = chunks[(leadingChunk + j) % M_CHUNK_COUNT];
            if(chunk.free) continue; //this shouldn't happen when running

            /* DEBUG */
            // ++actualChunkCount;
            /* END DEBUG */

            for(int item_num = 0; item_num != chunk.item_count; ++item_num) {
                merger.addItem(chunk.items[item_num], data, chunk.req_len);
            }
            chunk.free = 1;
        }

        // std::cout << "merger.cpp: Actual chunk count for this iteration was " << actualChunkCount << '\n';
    }
    merger.mergeAll();
    // merger.debugLog();

    /*
     * for each item
     *  for each log item in item
     *      copy data from data file to output data file
     */

    // std::cout << "merger.cpp: merging data...\n";
    auto& items = merger.getItems();
    for(auto iter = items.begin(); iter != items.end(); ++iter) {
        /*
         * if (curOutOffset + item.getLength() > maxOutSize) 
         *      for each item before this item
         *          destage the item to final storage
         *          remove it from the merger
         */

        auto& item = *iter;
        if(curOutOffset + item.getLength() > maxSize) {
            for(auto jter = items.begin(); jter != iter; ++jter) {
                auto& subItem = jter->getLogItems()[0];
                fileOut.seekp(jter->getBaseOffset());
                fileOut.write(static_cast<char*>(subItem.sourceData), jter->getLength());
            }

            // remove items
            // update length/base/data DONT NEED TO we're taking out an entire element
        }

        item.setDataOffset(curOutOffset);

        auto& logItems = item.getLogItems();
        int length = item.getLength() / logItems.size();
        for(const auto& logItem : logItems) {

            /*
            std::cout << "\tcopying from " << logItem.sourceData << " + " << logItem.item.data_offset <<
                            " to " << outData << " + " << curOutOffset <<
                            ", data is " << *(static_cast<int*>(logItem.sourceData) + logItem.item.data_offset / 4) << '\n';
                            */
            std::memcpy(static_cast<char*>(outData) + curOutOffset,
                    static_cast<char*>(logItem.sourceData) + logItem.item.data_offset,
                    length);

            curOutOffset += length;
        }

        // now that all of the items are meregd, update to the new backing item
        logItems.clear(); 
        logItems.emplace_back(TaggedItem{m_item{item.getDataOffset(), item.getBaseOffset()}, outData});
    }
    return merger;
}

Merger::Merger(std::size_t initialBackingSize) {
    items.reserve(initialBackingSize);
}

void Merger::addItem(const m_item& item, void* data, uint64_t length) {
    
    /*
    std::cout << "Creating new merge item:\n";
    std::cout << "\titem.target_offset: " << item.target_offset << '\n';
    std::cout << "\titem.data_offset: " << item.data_offset << '\n';
    std::cout << "\tlength: " << length << '\n';
    std::cout << "\tsourceData: " << data << '\n';
    */

    MergerItem newMergerItem = MergerItem{item, data, length};

    auto foundItemIter = std::lower_bound(items.begin(), items.end(), newMergerItem);

    // not contained
    if(foundItemIter == items.end() || *foundItemIter != newMergerItem) {
        // std::cout << "==!! No mergeable items found, adding new\n";
        items.emplace(foundItemIter, std::move(newMergerItem));
        return;
    }

    // std::cout << "==!! Mergeable item found! Merging.\n";
    foundItemIter->merge(newMergerItem);
}

void Merger::addItemNoMerge(const m_item& item, void* data, uint64_t length) {
    MergerItem newMergerItem = MergerItem{item, data, length}; 

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
            std::cout << "\tSource Data: " << subItem.sourceData << '\n';
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

