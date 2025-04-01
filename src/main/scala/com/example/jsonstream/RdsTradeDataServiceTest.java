import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RdsTradeDataServiceTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private ResultSet resultSet;

    @Mock
    private Connection connection;

    @Mock
    private DatabaseMetaData databaseMetaData;

    private RdsTradeDataService rdsTradeDataService;

    @BeforeEach
    void setUp() throws SQLException {
        rdsTradeDataService = new RdsTradeDataService();
        rdsTradeDataService.setJdbcTemplate(jdbcTemplate);
        rdsTradeDataService.setDataSource(dataSource);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
    }

    @Test
    void testGetLatestTradeHistoryEvent_Oracle() throws SQLException {
        when(databaseMetaData.getDatabaseProductName()).thenReturn("Oracle");
        testCommonLogic();
    }

    @Test
    void testGetLatestTradeHistoryEvent_H2() throws SQLException {
        when(databaseMetaData.getDatabaseProductName()).thenReturn("H2");
        testCommonLogic();
    }

    private void testCommonLogic() throws SQLException {
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("trade_id")).thenReturn("tradeId");
        when(resultSet.getString("source_system")).thenReturn("sourceSystem");
        when(resultSet.getString("created_ts")).thenReturn("createdTs");
        when(resultSet.getString("reportableEvent")).thenReturn("reportableEvent");
        when(resultSet.getString("positionBP")).thenReturn("positionBP");
        when(resultSet.getString("positionCP")).thenReturn("positionCP");

        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenAnswer(invocation -> {
                    ResultSetExtractor<List<RdsTradeDataService.SubmissionResponseWithEventType>> extractor = invocation.getArgument(2);
                    return extractor.extractData(resultSet);
                });

        List<RdsTradeDataService.SubmissionResponseWithEventType> result =
                rdsTradeDataService.getLatestTradeHistoryEvent("tradeId", "sourceSystem");

        assertNotNull(result);
        assertEquals(1, result.size());
        RdsTradeDataService.SubmissionResponseWithEventType item = result.get(0);
        assertEquals("tradeId", item.getTradeId());
        assertEquals("sourceSystem", item.getSourceSystem());
        assertEquals("createdTs", item.getCreatedTs());
        assertEquals("reportableEvent", item.getReportableEvent());
        assertEquals("positionBP", item.getPositionBP());
        assertEquals("positionCP", item.getPositionCP());
    }

    @Test
    void testGetLatestTradeHistoryEvent_SQLException() throws SQLException {
        when(databaseMetaData.getDatabaseProductName()).thenReturn("H2"); // or "Oracle"

        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenThrow(new SQLException("Database error"));

        List<RdsTradeDataService.SubmissionResponseWithEventType> result =
                rdsTradeDataService.getLatestTradeHistoryEvent("tradeId", "sourceSystem");

        assertNull(result);
    }
}