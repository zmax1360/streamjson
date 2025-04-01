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

    // ... (other mocks and variables)

    @Test
    void testGetLatestTradeHistoryEvent_Success() throws SQLException {
        when(resultSet.next()).thenReturn(true, false);
        // ... (other resultSet mocks)

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
        // ... (other assertions)
    }

    @Test
    void testGetLatestTradeHistoryEvent_NoResult() throws SQLException {
        when(resultSet.next()).thenReturn(false);

        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenAnswer(invocation -> {
                    ResultSetExtractor<List<RdsTradeDataService.SubmissionResponseWithEventType>> extractor = invocation.getArgument(2);
                    return extractor.extractData(resultSet);
                });

        List<RdsTradeDataService.SubmissionResponseWithEventType> result =
                rdsTradeDataService.getLatestTradeHistoryEvent("tradeId", "sourceSystem");

        assertNull(result);
    }
    // ... (rest of tests)
}