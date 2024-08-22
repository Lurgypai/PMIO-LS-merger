#include <fcntl.h>
#include <fstream>
#include <iterator>
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
        void* outData, int& curOutOffset, int maxDataSize,
        std::ofstream& fileOut) {

    Merger merger{2048};

    // m-log merge
    std::cout << "merger.cpp: Merging metadata...\n";
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
                merger.addItem(chunk.items[item_num], data, chunk.req_len, maxDataSize);
            }
            chunk.free = 1;
        }

        // std::cout << "merger.cpp: Actual chunk count for this iteration was " << actualChunkCount << '\n';
    }
    std::cout << "merger.cpp: mergeAll on " << merger.getItems().size() << " items." << std::endl;
    merger.mergeAll(maxDataSize);
    std::cout << "merger.cpp: Metadata merge complete.\n";
    // merger.debugLog();

    /*
     * for each item
     *  for each log item in item
     *      copy data from data file to output data file
     */

    std::cout << "merger.cpp: Merging data...\n";
    auto& items = merger.getItems();
    for(auto iter = items.begin(); iter != items.end(); ++iter) {

        auto& item = *iter;

        // check for full buffer
        // std::cout << "New write to " << curOutOffset << " + " << item.getLength() << " / " << maxDataSize;
        if(curOutOffset + item.getLength() > maxDataSize) {
            std::cout << "Buffer full, triggered early destage.\n";
            for(auto jter = items.begin(); jter != iter; ++jter) {
                auto& subItem = jter->getLogItems()[0];
                fileOut.seekp(jter->getBaseOffset());
                fileOut.write(static_cast<char*>(subItem.sourceData), jter->getLength());
            }
            iter = items.erase(items.begin(), iter);
            item = *iter;

            if(item.getLength() > maxDataSize) std::cout << "merger.cpp: WARNING: Large item detected.\n";
            
            curOutOffset = 0;
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
    std::cout << "merger.cpp: Data merge complete.\n";
    return merger;
}

Merger::Merger(std::size_t initialBackingSize) {
    items.reserve(initialBackingSize);
}

void Merger::addItem(const m_item& item, void* data, uint64_t length, int maxDataSize) {
    
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
    // only merge if the merged output isn't larger than the entire data buffer
    if(foundItemIter->getLength() + newMergerItem.getLength() <= maxDataSize) foundItemIter->merge(newMergerItem);
    else items.emplace(foundItemIter, std::move(newMergerItem));
}

void Merger::addItemNoMerge(const m_item& item, void* data, uint64_t length) {
    MergerItem newMergerItem = MergerItem{item, data, length}; 

    auto foundItemIter = std::lower_bound(items.begin(), items.end(), newMergerItem);
    items.emplace(foundItemIter, std::move(newMergerItem)); 
}

void Merger::mergeAll(int maxSize) {
    for(auto iter = items.rbegin(); iter != items.rend();) {
        auto prevIter = iter++;
        // one item
        if(iter == items.rend()) return;

        std::cout << "Attempting to merge " << iter->getBaseOffset() << " + " << iter->getLength() <<
                    " onto " << prevIter->getBaseOffset() << '\n';

        if(*prevIter == *iter && prevIter->getLength() + iter->getLength() <= maxSize) {
            iter->merge(*prevIter);
            iter = std::make_reverse_iterator(items.erase(iter.base()));
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

