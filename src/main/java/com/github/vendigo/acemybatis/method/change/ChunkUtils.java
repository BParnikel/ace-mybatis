package com.github.vendigo.acemybatis.method.change;

import com.github.vendigo.acemybatis.config.AceConfig;
import com.github.vendigo.acemybatis.parser.ParamsHolder;

public class ChunkUtils {
    public static boolean enoughForAsync(ParamsHolder params, AceConfig config) {
        return params.getEntities().size() > 2 * config.getUpdateChunkSize();
    }

    public static ChunkConfig buildChunkConfig(int numberOfEntities, int chunkSize) {
        int chunkNumber = numberOfEntities / chunkSize;
        if (numberOfEntities % chunkSize != 0) {
            chunkNumber++;
        }
        return new ChunkConfig(chunkNumber);
    }
}
