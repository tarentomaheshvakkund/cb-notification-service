package com.igot.cb.peervalidationcleanup.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.igot.common.cassandra.CassandraOperation;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PeerValidationCleanupServiceImpl}.
 *
 * <p>Tests call {@code processCleanupEvents} directly (package-private) to validate
 * Cassandra deletion logic independently of Kafka infrastructure. Kafka-level tests
 * stub {@code createKafkaConsumer()} via a Mockito spy.
 */
@ExtendWith(MockitoExtension.class)
class PeerValidationCleanupServiceImplTest {

    @Mock
    private CbServerProperties cbServerProperties;
    @Mock
    private CassandraOperation cassandraOperation;

    private PeerValidationCleanupServiceImpl service;

    private static final LocalDate TARGET_DATE = LocalDate.of(2024, 6, 1);
    private static final String USER_ID = "user-001";
    private static final String NOTIFICATION_ID = "notif-001";
    private static final String CREATED_AT_ISO = "2024-06-01T10:00:00Z";
    private static final String TOPIC = "test.topic";

    @BeforeEach
    void setUp() {
        service = spy(new PeerValidationCleanupServiceImpl(cbServerProperties, cassandraOperation, new ObjectMapper()));
        lenient().when(cbServerProperties.getPeerReviewAssignedExcludedStatuses())
                .thenReturn(List.of(Constants.STATUS_APPROVED, Constants.STATUS_REJECTED));
        lenient().when(cbServerProperties.getCleanupBatchSize()).thenReturn(100);
        lenient().when(cbServerProperties.getCleanupThreadPoolSize()).thenReturn(2);
        // Table names — match the production defaults so assertions against Constants still hold
        lenient().when(cbServerProperties.getCleanupTableRequests())
                .thenReturn(Constants.TABLE_PEER_VALIDATION_REQUESTS);
        lenient().when(cbServerProperties.getCleanupTableReviews())
                .thenReturn(Constants.TABLE_PEER_VALIDATION_REVIEWS);
        lenient().when(cbServerProperties.getCleanupTableAudit())
                .thenReturn(Constants.TABLE_PEER_VALIDATION_CLEANUP_FAILURES);
        // Time-window and scheduling settings
        lenient().when(cbServerProperties.getCleanupWindowStartTime()).thenReturn("00:01:00");
        lenient().when(cbServerProperties.getCleanupWindowEndTime()).thenReturn("23:59:59");
        lenient().when(cbServerProperties.getCleanupDayOffset()).thenReturn(1L);
        lenient().when(cbServerProperties.getCleanupPollTimeoutSeconds()).thenReturn(5);
        lenient().when(cbServerProperties.getCleanupExecutorShutdownTimeoutMinutes()).thenReturn(10);
        lenient().when(cbServerProperties.getCleanupAuditDeletionPrefix()).thenReturn("DELETED: ");
    }

    @Nested
    @DisplayName("processCleanupEvents – validation failures")
    class ValidationFailures {

        @Test
        @DisplayName("empty list returns zero summary")
        void emptyList_returnsZeroSummary() {
            var summary = service.processCleanupEvents(List.of(), TARGET_DATE);
            assertThat(summary.eligible()).isZero();
            assertThat(summary.deleted()).isZero();
            assertThat(summary.failed()).isZero();
        }

        @Test
        @DisplayName("invalid JSON increments failed count but does not insert to Cassandra")
        void invalidJson_incrementsFailedCount() {
            var summary = service.processCleanupEvents(List.of("not-json{{{"), TARGET_DATE);
            assertThat(summary.failed()).isEqualTo(1);
            assertThat(summary.eligible()).isZero();
            verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
        }

        @Test
        @DisplayName("missing userId increments failed count")
        void missingUserId_incrementsFailed() {
            String json = buildJson(null, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.failed()).isEqualTo(1);
            assertThat(summary.eligible()).isZero();
        }

        @Test
        @DisplayName("blank notificationId increments failed count")
        void blankNotificationId_incrementsFailed() {
            String json = buildJson(USER_ID, "  ", CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.failed()).isEqualTo(1);
        }

        @Test
        @DisplayName("unknown subCategory increments failed count")
        void unknownSubCategory_incrementsFailed() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "UNKNOWN_CATEGORY", Constants.STATUS_SUBMITTED);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.failed()).isEqualTo(1);
        }

        @Test
        @DisplayName("invalid createdAt increments failed count")
        void invalidCreatedAt_incrementsFailed() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, "not-a-date", "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.failed()).isEqualTo(1);
        }

        @Test
        @DisplayName("PENDING status for PEER_REVIEW_ASSIGNED is not eligible for deletion")
        void pendingStatus_peerReviewAssigned_notEligible() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO,
                    "PEER_REVIEW_ASSIGNED", Constants.STATUS_PENDING);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.failed()).isEqualTo(1);
            assertThat(summary.eligible()).isZero();
            verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        }

        @Test
        @DisplayName("SUBMITTED status for PEER_REVIEW_ASSIGNED is not eligible for deletion")
        void submittedStatus_peerReviewAssigned_notEligible() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO,
                    "PEER_REVIEW_ASSIGNED", Constants.STATUS_SUBMITTED);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.failed()).isEqualTo(1);
            assertThat(summary.eligible()).isZero();
            verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        }

        @Test
        @DisplayName("null status for PEER_REVIEW_ASSIGNED is not eligible for deletion")
        void nullStatus_peerReviewAssigned_notEligible() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_REVIEW_ASSIGNED", null);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.failed()).isEqualTo(1);
            assertThat(summary.eligible()).isZero();
            verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        }
    }

    @Nested
    @DisplayName("processCleanupEvents – successful deletions")
    class SuccessfulDeletions {

        @Test
        @DisplayName("PEER_EVALUATION_ASSIGNED event deletes from requests table and notifications")
        void peerEvaluationAssigned_deletesCorrectTables() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.eligible()).isEqualTo(1);
            assertThat(summary.deleted()).isEqualTo(1);
            assertThat(summary.failed()).isZero();
            verify(cassandraOperation).deleteRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_PEER_VALIDATION_REQUESTS,
                    Map.of(Constants.USER_ID, USER_ID, Constants.NOTIFICATION_ID, NOTIFICATION_ID)
            );
            verify(cassandraOperation, never()).deleteRecord(
                    eq(Constants.KEYSPACE_SUNBIRD),
                    eq(Constants.TABLE_USER_NOTIFICATION),
                    anyMap()
            );
        }

        @Test
        @DisplayName("PEER_REVIEW_ASSIGNED event routes to reviews table")
        void peerReviewAssigned_routesToReviewsTable() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_REVIEW_ASSIGNED", Constants.STATUS_APPROVED);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.deleted()).isEqualTo(1);
            verify(cassandraOperation).deleteRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_PEER_VALIDATION_REVIEWS,
                    Map.of(Constants.USER_ID, USER_ID, Constants.NOTIFICATION_ID, NOTIFICATION_ID)
            );
        }

        @Test
        @DisplayName("REJECTED status for PEER_REVIEW_ASSIGNED is eligible for deletion")
        void rejectedStatus_peerReviewAssigned_isEligibleForDeletion() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_REVIEW_ASSIGNED", Constants.STATUS_REJECTED);
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.deleted()).isEqualTo(1);
            assertThat(summary.failed()).isZero();
            verify(cassandraOperation).deleteRecord(
                    eq(Constants.KEYSPACE_SUNBIRD),
                    eq(Constants.TABLE_PEER_VALIDATION_REVIEWS),
                    anyMap()
            );
        }

        @Test
        @DisplayName("multiple successful events produce a single bulk audit insert call")
        void multipleSuccessful_bulkAuditCalledOnce() {
            String eval = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            String review = buildJson("user-002", "notif-002", CREATED_AT_ISO, "PEER_REVIEW_ASSIGNED", Constants.STATUS_APPROVED);
            service.processCleanupEvents(List.of(eval, review), TARGET_DATE);
            verify(cassandraOperation, times(1)).insertBulkRecord(
                    eq(Constants.KEYSPACE_SUNBIRD),
                    eq(Constants.TABLE_PEER_VALIDATION_CLEANUP_FAILURES),
                    anyList()
            );
        }

        @Test
        @DisplayName("subCategory matching is case-insensitive")
        void subCategory_caseInsensitiveMatch() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "peer_evaluation_assigned", Constants.STATUS_SUBMITTED);

            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);

            assertThat(summary.deleted()).isEqualTo(1);
        }

        @Test
        @DisplayName("successful deletion inserts a bulk audit record into the cleanup audit table")
        void successfulDelete_insertsBulkAuditRecord() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            service.processCleanupEvents(List.of(json), TARGET_DATE);
            verify(cassandraOperation, times(1)).insertBulkRecord(
                    eq(Constants.KEYSPACE_SUNBIRD),
                    eq(Constants.TABLE_PEER_VALIDATION_CLEANUP_FAILURES),
                    anyList()
            );
        }

        @Test
        @DisplayName("multiple events produce correct aggregate counts")
        void multipleEvents_correctCounts() {
            String valid1 = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            String valid2 = buildJson("user-002", "notif-002", CREATED_AT_ISO, "PEER_REVIEW_ASSIGNED", Constants.STATUS_APPROVED);
            String invalid = "bad-json";

            var summary = service.processCleanupEvents(List.of(valid1, valid2, invalid), TARGET_DATE);

            assertThat(summary.eligible()).isEqualTo(2);
            assertThat(summary.deleted()).isEqualTo(2);
            assertThat(summary.failed()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("processCleanupEvents – Cassandra errors")
    class CassandraErrors {

        @Test
        @DisplayName("Cassandra deleteRecord throws increments failed count and skips audit insert")
        void cassandraThrows_incrementsFailedCountAndSkipsAudit() {
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            doThrow(new RuntimeException("Cassandra unavailable"))
                    .when(cassandraOperation).deleteRecord(anyString(), anyString(), anyMap());
            var summary = service.processCleanupEvents(List.of(json), TARGET_DATE);
            assertThat(summary.eligible()).isEqualTo(1);
            assertThat(summary.failed()).isEqualTo(1);
            assertThat(summary.deleted()).isZero();
            verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        }
    }

    @Nested
    @DisplayName("runCleanup – orchestration")
    class RunCleanup {

        @Mock
        private KafkaConsumer<String, String> mockConsumer;

        private PeerValidationCleanupServiceImpl kafkaService;

        @BeforeEach
        void setUpKafkaService() {
            kafkaService = spy(new PeerValidationCleanupServiceImpl(
                    cbServerProperties, cassandraOperation, new ObjectMapper()));
            doReturn(mockConsumer).when(kafkaService).createKafkaConsumer();
        }

        @AfterEach
        void closeMockConsumer() {
            mockConsumer.close();
        }

        @Test
        @DisplayName("no partitions for topic → zero events processed, no Cassandra calls")
        void noPartitions_zeroEvents() {
            when(cbServerProperties.getCleanupKafkaTopics()).thenReturn(TOPIC);
            when(mockConsumer.partitionsFor(TOPIC)).thenReturn(Collections.emptyList());
            kafkaService.runCleanup(Instant.now());
            verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        }

        @Test
        @DisplayName("consumer returns empty poll → no Cassandra calls")
        void emptyPoll_noCassandraCalls() {
            TopicPartition tp = new TopicPartition(TOPIC, 0);
            PartitionInfo pi = new PartitionInfo(TOPIC, 0, null, null, null);
            when(cbServerProperties.getCleanupKafkaTopics()).thenReturn(TOPIC);
            when(mockConsumer.partitionsFor(TOPIC)).thenReturn(List.of(pi));
            when(mockConsumer.offsetsForTimes(anyMap()))
                    .thenReturn(Map.of(tp, new OffsetAndTimestamp(0L, 0L)));
            when(mockConsumer.endOffsets(anyList())).thenReturn(Map.of(tp, 0L));
            kafkaService.runCleanup(Instant.now());
            verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        }

        @Test
        @DisplayName("single valid event in window → one Cassandra delete pair")
        void singleEventInWindow_deletesCassandraRows() {
            TopicPartition tp = new TopicPartition(TOPIC, 0);
            PartitionInfo pi = new PartitionInfo(TOPIC, 0, null, null, null);
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            LocalDate yesterday = LocalDate.now(ZoneOffset.UTC).minusDays(1);
            long windowStartMs = yesterday.atTime(0, 1, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
            long windowEndMs = yesterday.atTime(23, 59, 59).toInstant(ZoneOffset.UTC).toEpochMilli();
            long midpointMs = (windowStartMs + windowEndMs) / 2;
            ConsumerRecord<String, String> kafkaRecord =
                    new ConsumerRecord<>(TOPIC, 0, 0L, midpointMs,
                            TimestampType.CREATE_TIME, -1L, -1, -1, null, json);
            ConsumerRecords<String, String> batch =
                    new ConsumerRecords<>(Map.of(tp, List.of(kafkaRecord)));
            when(cbServerProperties.getCleanupKafkaTopics()).thenReturn(TOPIC);
            when(mockConsumer.partitionsFor(TOPIC)).thenReturn(List.of(pi));
            when(mockConsumer.offsetsForTimes(anyMap()))
                    .thenReturn(Map.of(tp, new OffsetAndTimestamp(0L, windowStartMs)));
            when(mockConsumer.endOffsets(anyList())).thenReturn(Map.of(tp, 2L));
            doReturn(batch).doReturn(ConsumerRecords.<String, String>empty()).when(mockConsumer).poll(any(Duration.class));
            kafkaService.runCleanup(Instant.now());
            verify(cassandraOperation, times(1)).deleteRecord(
                    eq(Constants.KEYSPACE_SUNBIRD),
                    eq(Constants.TABLE_PEER_VALIDATION_REQUESTS),
                    anyMap()
            );
            verify(cassandraOperation, never()).deleteRecord(
                    eq(Constants.KEYSPACE_SUNBIRD),
                    eq(Constants.TABLE_USER_NOTIFICATION),
                    anyMap()
            );
        }
    }

    @Nested
    @DisplayName("parseTopics – configuration edge cases (via runCleanup)")
    class ParseTopics {

        @Mock
        private KafkaConsumer<String, String> mockConsumer;

        private PeerValidationCleanupServiceImpl kafkaService;

        @BeforeEach
        void setUpKafkaService() {
            kafkaService = spy(new PeerValidationCleanupServiceImpl(
                    cbServerProperties, cassandraOperation, new ObjectMapper()));
            doReturn(mockConsumer).when(kafkaService).createKafkaConsumer();
        }

        @AfterEach
        void closeMockConsumer() {
            mockConsumer.close();
        }

        @Test
        @DisplayName("topics with blank entries are filtered out")
        void blankTopicEntries_filtered() {
            when(cbServerProperties.getCleanupKafkaTopics()).thenReturn("topic-a,,  ,topic-b");
            when(mockConsumer.partitionsFor("topic-a")).thenReturn(Collections.emptyList());
            when(mockConsumer.partitionsFor("topic-b")).thenReturn(Collections.emptyList());
            kafkaService.runCleanup(Instant.now());
            verify(mockConsumer).partitionsFor("topic-a");
            verify(mockConsumer).partitionsFor("topic-b");
        }
    }

    @Nested
    @DisplayName("processRecord – window boundary decisions")
    class ProcessRecord {

        @Mock
        private KafkaConsumer<String, String> mockConsumer;

        private PeerValidationCleanupServiceImpl kafkaService;

        @BeforeEach
        void setUpKafkaService() {
            kafkaService = spy(new PeerValidationCleanupServiceImpl(
                    cbServerProperties, cassandraOperation, new ObjectMapper()));
            doReturn(mockConsumer).when(kafkaService).createKafkaConsumer();
        }

        @AfterEach
        void closeMockConsumer() {
            mockConsumer.close();
        }

        @Test
        @DisplayName("record after window end marks partition exhausted, not collected")
        void recordAfterWindowEnd_exhausted_notCollected() {
            TopicPartition tp = new TopicPartition(TOPIC, 0);
            PartitionInfo pi = new PartitionInfo(TOPIC, 0, null, null, null);
            LocalDate yesterday = LocalDate.now(ZoneOffset.UTC).minusDays(1);
            long windowEndMs = yesterday.atTime(23, 59, 59).toInstant(ZoneOffset.UTC).toEpochMilli();
            long afterWindowMs = windowEndMs + 10_000;
            String json = buildJson(USER_ID, NOTIFICATION_ID, CREATED_AT_ISO, "PEER_EVALUATION_ASSIGNED", Constants.STATUS_SUBMITTED);
            ConsumerRecord<String, String> lateRecord =
                    new ConsumerRecord<>(TOPIC, 0, 0L, afterWindowMs,
                            TimestampType.CREATE_TIME, -1L, -1, -1, null, json);
            ConsumerRecords<String, String> batch =
                    new ConsumerRecords<>(Map.of(tp, List.of(lateRecord)));
            when(cbServerProperties.getCleanupKafkaTopics()).thenReturn(TOPIC);
            when(mockConsumer.partitionsFor(TOPIC)).thenReturn(List.of(pi));
            long windowStartMs = yesterday.atTime(0, 1, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
            when(mockConsumer.offsetsForTimes(anyMap()))
                    .thenReturn(Map.of(tp, new OffsetAndTimestamp(0L, windowStartMs)));
            when(mockConsumer.endOffsets(anyList())).thenReturn(Map.of(tp, 5L));
            doReturn(batch).doReturn(ConsumerRecords.<String, String>empty()).when(mockConsumer).poll(any(Duration.class));
            kafkaService.runCleanup(Instant.now());
            verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        }
    }

    private String buildJson(String userId, String notificationId, String createdAt,
                             String subCategory, String status) {
        Map<String, Object> map = new HashMap<>();
        if (userId != null) map.put(Constants.USER_ID_FIELD, userId);
        if (notificationId != null) map.put(Constants.NOTIFICATION_ID_FIELD, notificationId);
        if (createdAt != null) map.put(Constants.CREATED_AT_FIELD, createdAt);
        if (subCategory != null) map.put(Constants.SUB_CATEGORY_FIELD, subCategory);
        if (status != null) map.put(Constants.STATUS_FIELD, status);
        try {
            return new ObjectMapper().writeValueAsString(map);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
