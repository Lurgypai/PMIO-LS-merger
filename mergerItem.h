#ifndef MERGER_ITEM_H
#define MERGER_ITEM_H

#include <vector>
#include <string>

#include <cstdint>

#include "mChunk.h"

// associates an item with its source datafile
struct TaggedItem {
    m_item item;
    std::string sourceDataFile;

    bool operator<(const TaggedItem& other) const;
};


/*
 * Change this to instead store
 * current offset and length of joined IO
 * sorted list of log items that make up the new IO
 */

// missing move ctor
// is there a default?
class MergerItem {
public:
    MergerItem(const m_item& startingItem, const std::string& sourceDataFile, uint64_t length);
    
    // this item comes before the other in the file
    bool operator<(const MergerItem& other) const;
    
    // the items can be merged
    bool operator==(const MergerItem& other) const;

    // the items cannot be merged
    bool operator!=(const MergerItem& other) const;

    void merge(const MergerItem& other);

    uint64_t getBaseOffset() const;
    void setBaseOffset(uint64_t offset);

    uint64_t getLength() const;
    void setLength(uint64_t length_);
    
    // end is computed from the base item stats
    uint64_t getEnd() const;

    const std::vector<TaggedItem>& getLogItems() const;
    std::vector<TaggedItem>& getLogItems();

    uint64_t getDataOffset() const;
    void setDataOffset(uint64_t offset_);
private:
    uint64_t offset;
    uint64_t length;    
    uint64_t data_offset; //offset into data file to read this entry from, not set until this entry has been committed to a file
                          //when committing to files, use the sub items to figure out where the source data for this new merged entry is

    std::vector<TaggedItem> items;
};

#endif
