#ifndef MERGER_H
#define MERGER_H

#include <cstring>
#include <vector>
#include <fstream>
#include "mergerItem.h"

class Merger {
public:
    explicit Merger(std::size_t initialBackingSize);
    // may leave item in a "moved from" state
    void addItem(const m_item& item, void* data, uint64_t length);
    void addItemNoMerge(const m_item& item, void* data, uint64_t length);
    void mergeAll();
    void clear();

    void debugLog() const;
    std::size_t getItemCount() const;
    const std::vector<MergerItem>& getItems() const;
    std::vector<MergerItem>& getItems();

private:
    std::vector<MergerItem> items;
};

// returns the newly merged merger
Merger MergeData(const std::vector<m_chunk*>& sourceMetadata,
        const std::vector<void*>& sourceData,
        const std::vector<int>& leadingChunks, const std::vector<int>& endChunks,
        void* outData, int& curOutOffset, int maxSize,
        std::ofstream& fileOut);

#endif
