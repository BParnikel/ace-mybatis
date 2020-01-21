package com.github.vendigo.acemybatis.method.change;

import com.github.vendigo.acemybatis.config.AceConfig;
import com.github.vendigo.acemybatis.parser.ParamsHolder;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import static com.github.vendigo.acemybatis.method.change.TestUtils.createParamsHolder;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ChangeHelperTest {
    @Mock
    private SqlSessionFactory sqlSessionFactory;
    @Mock
    private SqlSession sqlSession;
    @Mock
    private Connection connection;

    private Map<String, Object> otherParams;

    @Before
    public void setUp() throws Exception {
        otherParams = new HashMap<>();
        otherParams.put("param1", "value1");
        otherParams.put("param2", "value2");
    }

    @Test
    public void do4CommitsWhen10By3In2Threads() throws Exception {
        testApply(4, 10, 3, 2);
    }

    @Test
    public void do3CommitWhen50By100In3Threads() throws Exception {
        testApply(1, 50, 100, 3);
    }

    @Test
    public void do12CommitsWhen1000By100In4Threads() throws Exception {
        testApply(10, 1000, 100, 4);
    }

    private void testApply(int expectedCommits, int numberOfElements, int chunkSize, int threadCount) throws Exception {
        AceConfig aceConfig = new AceConfig(chunkSize, threadCount);
        ParamsHolder paramsHolder = createParamsHolder(numberOfElements);
        when(sqlSessionFactory.openSession(ExecutorType.BATCH)).thenReturn(sqlSession);
        when(sqlSession.getConnection()).thenReturn(connection);
        ChunkConfig chunkConfig = ChunkUtils.buildChunkConfig(numberOfElements, chunkSize);

        ChangeHelper.applyInParallel(aceConfig, (sqlSession, statementName, entity) -> {
                }, sqlSessionFactory,
                "insert", paramsHolder, chunkConfig);
        verify(sqlSession, times(expectedCommits)).commit();
    }
}
