#ifndef MERGER_H
#define MERGER_H

#include <cstring>
#include <vector>
#include "mergerItem.h"

class Merger {
public:
    explicit Merger(std::size_t initialBackingSize);
    // may leave item in a "moved from" state
    void addItem(const m_item& item, const std::string& sourceDataFile, uint64_t length);
    void addItemNoMerge(const m_item& item, const std::string& sourceDataFile, uint64_t length);
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
Merger MergeFiles(const std::vector<std::string>& sourceLogFiles,
        const std::vector<std::string>& sourceDataFiles,
        const std::string& mergedDataFile);

#endif
