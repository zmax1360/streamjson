package com.example.jsonstream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RdsTradeDataServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private ResultSet resultSet;

    private RdsTradeDataService rdsTradeDataService;

    @BeforeEach
    void setUp() {
        rdsTradeDataService = new RdsTradeDataService();
        rdsTradeDataService.setJdbcTemplate(jdbcTemplate);
    }

    @Test
    void testGetLatestTradeHistoryEvent_Success() throws SQLException {
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("trade_id")).thenReturn("tradeId");
        when(resultSet.getString("source_system")).thenReturn("sourceSystem");
        when(resultSet.getString("created_ts")).thenReturn("createdTs");
        when(resultSet.getString("reportableEvent")).thenReturn("reportableEvent");
        when(resultSet.getString("positionBP")).thenReturn("positionBP");
        when(resultSet.getString("positionCP")).thenReturn("positionCP");

        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenAnswer(invocation -> {
                    ResultSetExtractor<RdsTradeDataService.SubmissionResponseWithEventType> extractor = invocation.getArgument(2);
                    return extractor.extractData(resultSet);
                });

        RdsTradeDataService.SubmissionResponseWithEventType result =
                rdsTradeDataService.getLatestTradeHistoryEvent("tradeId", "sourceSystem");

        assertNotNull(result);
        assertEquals("tradeId", result.getTradeId());
        assertEquals("sourceSystem", result.getSourceSystem());
        assertEquals("createdTs", result.getCreatedTs());
        assertEquals("reportableEvent", result.getReportableEvent());
        assertEquals("positionBP", result.getPositionBP());
        assertEquals("positionCP", result.getPositionCP());
    }

    @Test
    void testGetLatestTradeHistoryEvent_NoResult() throws SQLException {
        when(resultSet.next()).thenReturn(false);

        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenAnswer(invocation -> {
                    ResultSetExtractor<RdsTradeDataService.SubmissionResponseWithEventType> extractor = invocation.getArgument(2);
                    return extractor.extractData(resultSet);
                });

        RdsTradeDataService.SubmissionResponseWithEventType result =
                rdsTradeDataService.getLatestTradeHistoryEvent("tradeId", "sourceSystem");

        assertNull(result);
    }

    @Test
    void testGetLatestTradeHistoryEvent_SQLException() throws SQLException {
        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenThrow(new SQLException("Database error"));

        RdsTradeDataService.SubmissionResponseWithEventType result =
                rdsTradeDataService.getLatestTradeHistoryEvent("tradeId", "sourceSystem");

        assertNull(result);
    }
}