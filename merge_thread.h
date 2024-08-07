#ifndef MERGE_THREAD_C_H
#define MERGE_THREAD_C_H

void start_merge_thread(const char* fileName,
        char** metadataFileNames,
        char** dataFileNames,
        int fileCount,
        const char* outDataFileName,
        int maxCHunkInUseCOunt, int maxMasterItemCount, int stripeSize);

void stop_merge_thread();

#endif
