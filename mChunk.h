#ifndef M_CHUNK_H
#define M_CHUNK_H

#include <cstdint>

#define M_ITEM_COUNT 16
#define M_CHUNK_COUNT 1024

typedef struct _m_item {
    uint64_t data_offset; // offset into data log where tmp data is stored
    uint64_t target_offset; // offset into output file where data should go
} m_item;

typedef struct _m_chunk {
    uint64_t next_chunk; //index into circular buffer
    uint64_t free : 1; // is in use
    uint64_t item_count : 63; // number of items in this chunk
    uint64_t stride; // space between items
    uint64_t req_len; // length of each request
    uint64_t st_offset; // offset from beginning of file
    m_item items[M_ITEM_COUNT];
} m_chunk;

#endif
