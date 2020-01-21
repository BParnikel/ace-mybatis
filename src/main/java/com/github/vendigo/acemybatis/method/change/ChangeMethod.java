package com.github.vendigo.acemybatis.method.change;

import com.github.vendigo.acemybatis.config.AceConfig;
import com.github.vendigo.acemybatis.method.AceMethod;
import com.github.vendigo.acemybatis.parser.ParamsHolder;
import com.github.vendigo.acemybatis.parser.ParamsParser;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.vendigo.acemybatis.utils.Validator.notNull;

/**
 * Implementation for change methods (insert/update/delete).
 */
public class ChangeMethod implements AceMethod {
    private static final Logger log = LoggerFactory.getLogger(ChangeMethod.class);

    private final String statementName;
    private final boolean async;
    private final MapperMethod.MethodSignature methodSignature;
    private final AceConfig config;
    private final ChangeFunction changeFunction;

    public ChangeMethod(String statementName, boolean async, MapperMethod.MethodSignature methodSignature,
                        AceConfig config, ChangeFunction changeFunction) {
        this.methodSignature = notNull(methodSignature);
        this.async = async;
        this.statementName = notNull(statementName);
        this.config = notNull(config);
        this.changeFunction = notNull(changeFunction);
    }

    @Override
    public Integer execute(SqlSessionFactory sqlSessionFactory, Object[] args) throws Exception {
        ParamsHolder params = ParamsParser.parseParams(config, methodSignature, args);
        int entities = params.getEntities().size();
        int chunkSize = config.getUpdateChunkSize();
        ChunkConfig chunkConfig = ChunkUtils.buildChunkConfig(entities, chunkSize);
        int result;
        if (async && ChunkUtils.enoughForAsync(params, config)) {
            log.info("[{}] Process {} records in {} thread(s) by chunks of size {}", statementName, entities, config.getThreadCount(), chunkSize);
            result = ChangeHelper.applyInParallel(config, changeFunction, sqlSessionFactory, statementName, params, chunkConfig);
        } else {
            log.info("[{}] Process {} records in 1 thread by chunks of size {}", statementName, entities, chunkSize);
            result = ChangeHelper.applySingleCore(config, changeFunction, sqlSessionFactory, statementName, params, chunkConfig);
        }
        log.info("[{}] Done", statementName);
        return result;
    }
}
