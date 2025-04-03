package com.example.jsonstream;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RdsTradeDataService {

    private static final Logger log = LoggerFactory.getLogger(RdsTradeDataService.class);
    private JdbcTemplate jdbcTemplate;

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<SubmissionResponseWithEventType> getLatestTradeHistoryEvent(String tradeId, String sourceSystem) {
        String sql = """
            select rv.trade_id,
                   rv.source_system,
                   rv.created_ts,
                   JSON_VALUE(rv.PAYLOAD, '$.tradeLifeCycleEvent.eventType')                          AS reportableEvent,
                   JSON_VALUE(rv.PAYLOAD, '$.reportingRegimes.EMIR.FCA.isBookingPartyTRPositionOpen') AS positionBP,
                   JSON_VALUE(rv.PAYLOAD, '$.reportingRegimes.EMIR.FCA.isCounterPartyTRPositionOpen') AS positionCP
            FROM REGULATORY_V2 rv
                     LEFT JOIN VENDOR_SUBMISSIONS vs ON vs.CORRELATION_ID = rv.CORRELATION_ID and vs.regime = 'EMIR_FCA'
                     LEFT JOIN VENDOR_RESPONSES vr ON vs.SUBMISSION_ID = vr.SUBMISSION_ID and vr.regime = 'EMIR_FCA'
            where vr.RESPONSE_TYPE = 'ACK'
              and rv.TRADE_ID = ?
              and rv.SOURCE_SYSTEM = ?
            ORDER BY rv.created_ts DESC
            LIMIT 1;
            """;
        try {
            Optional<JdbcTemplate> jdbcTemplateOptional = Optional.ofNullable(jdbcTemplate);
            if (jdbcTemplateOptional.isPresent()) {
                PreparedStatementSetter preparedStatementSetter = ps -> {
                    ps.setString(1, tradeId);
                    ps.setString(2, sourceSystem);
                };
                Optional<List<SubmissionResponseWithEventType>> submissionResponseWithEventTypes =
                        Optional.ofNullable(jdbcTemplateOptional.get().query(sql, preparedStatementSetter,  rs ->{
                            List<SubmissionResponseWithEventType> list = new ArrayList<>();
                            while (rs.next()){
                               list.add(new SubmissionResponseWithEventType(
                                       rs.getString("trade_id"),
                                       rs.getString("source_system"),
                                       rs.getString("created_ts"),
                                       rs.getString("reportableEvent"),
                                       rs.getString("positionBP"),
                                       rs.getString("positionCP"));
                            }
                            return list;
                        }));
                        if (submissionResponseWithEventTypes.isPresent()){
                            return submissionResponseWithEventTypes.get()
                        }
            }
        } catch (Exception ex) {
            log.error("Error getLatestTradeHistoryEvent.", ex);
        }
        return null;
    }

    @AllArgsConstructor
    @Getter
    public static class SubmissionResponseWithEventType {
        private String tradeId;
        private String sourceSystem;
        private String createdTs;
        private String reportableEvent;
        private String positionBP;
        private String positionCP;
    }
    @Test
    void testGetLatestTradeHistoryEvent_JdbcTemplatePresent_QueryReturnsEmptyList() {
        // Arrange
        when(jdbcTemplate.query(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any(org.springframework.jdbc.core.PreparedStatementSetter.class),
                org.mockito.ArgumentMatchers.any(org.springframework.jdbc.core.ResultSetExtractor.class)))
                .thenReturn(java.util.Collections.emptyList()); // Simulate query returning empty list

        // Act
        List<RdsTradeDataService.SubmissionResponseWithEventType> result =
                rdsTradeDataService.getLatestTradeHistoryEvent("tradeId", "sourceSystem");

        // Assert
        assertNull(result); // Should return null when query returns empty list
    }
}
