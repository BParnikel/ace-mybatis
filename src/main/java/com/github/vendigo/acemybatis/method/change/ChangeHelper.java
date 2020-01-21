package com.github.vendigo.acemybatis.method.change;

import com.github.vendigo.acemybatis.config.AceConfig;
import com.github.vendigo.acemybatis.parser.ParamsHolder;
import com.github.vendigo.acemybatis.proxy.RuntimeExecutionException;
import com.google.common.collect.Lists;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ChangeHelper {
    private static final Logger log = LoggerFactory.getLogger(ChangeHelper.class);

    private ChangeHelper() {
    }

    static Integer applyInParallel(AceConfig config, ChangeFunction changeFunction,
                                   SqlSessionFactory sqlSessionFactory, String statementName,
                                   ParamsHolder params, ChunkConfig chunkConfig) {
        ExecutorService executorService = Executors.newFixedThreadPool(config.getThreadCount());
        int result = 0;

        try {
            List<List<Object>> chunks = Lists.partition(params.getEntities(), config.getUpdateChunkSize());
            List<Future<Integer>> tasks = chunks.stream()
                    .map(chunk -> new ParamsHolder(chunk, params.getOtherParams()))
                    .map(paramHolder -> new ChangeTask(config, changeFunction, sqlSessionFactory, statementName, paramHolder, chunkConfig))
                    .map(executorService::submit)
                    .collect(Collectors.toList());

            for (Future<Integer> task : tasks) {
                result += getFromFuture(task);
            }
        } finally {
            executorService.shutdown();
        }
        return result;
    }

    static int applySingleCore(AceConfig config, ChangeFunction changeFunction,
                               SqlSessionFactory sqlSessionFactory, String statementName,
                               ParamsHolder params, ChunkConfig chunkConfig) throws SQLException {
        int i = 0;
        List<Object> entities = params.getEntities();
        Map<String, Object> otherParams = params.getOtherParams();

        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            Connection connection = sqlSession.getConnection();
            connection.setAutoCommit(false);
            for (Object entity : entities) {
                changeFunction.apply(sqlSession, statementName, formatParam(config, entity, otherParams));
                i++;
                if (i % config.getUpdateChunkSize() == 0) {
                    sqlSession.commit();
                    connection.commit();
                    log.info("Processed batch {}/{}", chunkConfig.getCurrentChunk(), chunkConfig.getTotalChunks());
                }
            }
            if (i % config.getUpdateChunkSize() != 0) {
                sqlSession.commit();
                connection.commit();
                log.info("Processed batch {}/{}", chunkConfig.getCurrentChunk(), chunkConfig.getTotalChunks());
            }
        }
        //In batch mode sqlSession returns incorrect number of affected rows. We presume that one call changes one row.
        return i;
    }

    static void changeChunk(SqlSession sqlSession, List<Object> chunk, String statementName,
                            ChangeFunction changeFunction) {
        for (Object entity : chunk) {
            changeFunction.apply(sqlSession, statementName, entity);
        }
        sqlSession.commit();
    }

    private static Object formatParam(AceConfig config, Object entity, Map<String, Object> otherParams) {
        if (otherParams.isEmpty()) {
            return entity;
        } else {
            Map<String, Object> param = new HashMap<>();
            param.putAll(otherParams);
            param.put(config.getElementName(), entity);
            return param;
        }
    }

    private static Integer getFromFuture(Future<Integer> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeExecutionException(e);
        }
    }
}
