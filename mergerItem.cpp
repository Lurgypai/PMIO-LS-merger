#include <algorithm>
#include <iterator>
#include <vector>

#include "mergerItem.h"

bool TaggedItem::operator<(const TaggedItem& other) const {
    return item.target_offset < other.item.target_offset;
}

MergerItem::MergerItem(const m_item& startingItem, void* sourceData, uint64_t length_) :
    length{length_},
    offset{startingItem.target_offset},
    items{TaggedItem{startingItem, sourceData}}
{}

bool MergerItem::operator<(const MergerItem& other) const {
    return offset + length < other.offset;
}

bool MergerItem::operator==(const MergerItem& other) const {
    auto end = offset + length;
    return end == other.offset ||
        other.getEnd() == offset;
}

bool MergerItem::operator!=(const MergerItem& other) const {
    return !(*this == other);
}

void MergerItem::merge(const MergerItem& other) {
    /*
    std::cout << "Merging:\n";
    std::cout << "\tbase offset: " << offset << '\n';
    std::cout << "\tbase length: " << length << '\n';
    std::cout << "\tother offset: " << other.offset << '\n';
    std::cout << "\tother length: " << other.length << '\n';
    */

    if(other.offset < offset) offset = other.offset;
    length += other.length;

    std::vector<TaggedItem> temp{};
    temp.reserve(items.size() + other.items.size());

    std::merge(items.begin(), items.end(),
            other.items.begin(), other.items.end(),
            std::back_inserter(temp));

    items = std::move(temp);

    /*
    std::cout << "Merging complete, result:\n";
    std::cout << "\toffset: " << offset << '\n';
    std::cout << "\tlength: " << length << '\n';
    */
}

uint64_t MergerItem::getBaseOffset() const {
    return offset;
}

void MergerItem::setBaseOffset(uint64_t offset_) {
    offset = offset_;
}

uint64_t MergerItem::getLength() const {
    return length;
}

void MergerItem::setLength(uint64_t length_) {
    length = length_;
}

uint64_t MergerItem::getEnd() const {
    return offset + length;
}

const std::vector<TaggedItem>& MergerItem::getLogItems() const {
    return items;
}

std::vector<TaggedItem>& MergerItem::getLogItems() {
    return items;
}

uint64_t MergerItem::getDataOffset() const {
    return data_offset;
}

void MergerItem::setDataOffset(uint64_t offset_) {
    data_offset = offset_;
}
