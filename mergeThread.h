#ifndef MERGE_THREAD_H
#define MERGE_THREAD_H

#include <vector>
#include <string>

namespace MergeThread {
    void StartMergeThread(const std::string targetFilename,
            std::vector<std::string> metadataFileNames,
            std::vector<std::string> dataFileNames,
            const std::string outDataFilename,
            int maxChunkInUseCount, int maxMasterItemCount, int stripeSize);
    void PauseMergeThread();
    void UnpauseMergeThread();
    void StopMergeThread();
}

#endif
