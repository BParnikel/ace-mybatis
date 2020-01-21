package com.github.vendigo.acemybatis.method.change;

import java.util.concurrent.atomic.AtomicInteger;

public class ChunkConfig {
    private final int totalChunks;
    private final AtomicInteger processedChunkNumber;

    ChunkConfig(int totalChunks) {
        this.processedChunkNumber = new AtomicInteger();
        this.totalChunks = totalChunks;
    }

    int getTotalChunks() {
        return totalChunks;
    }

    int getCurrentChunk() {
        return processedChunkNumber.incrementAndGet();
    }
}
