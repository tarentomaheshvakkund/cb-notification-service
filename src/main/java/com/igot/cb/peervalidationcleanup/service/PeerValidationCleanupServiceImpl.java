package com.igot.cb.peervalidationcleanup.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.igot.common.cassandra.CassandraOperation;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Default implementation of {@link PeerValidationCleanupService}.
 *
 * <p>Reads time-windowed Kafka events for the previous calendar day, deserializes each
 * event, and removes the corresponding row from the peer-validation action table in
 * Cassandra. Every successful deletion is audited by inserting a record into
 * {@code peer_validation_cleanup_failures} for traceability. Failures are only logged
 * and never written to the database.
 */
@Service
@Slf4j
public class PeerValidationCleanupServiceImpl implements PeerValidationCleanupService {


    private static final TypeReference<Map<String, Object>> EVENT_TYPE_REF = new TypeReference<>() {
    };

    private final CbServerProperties cbServerProperties;
    private final CassandraOperation cassandraOperation;
    private final ObjectMapper objectMapper;

    /**
     * Epoch-ms lower and upper bounds for a single day's cleanup window.
     */
    record TimeWindow(long startMs, long endMs) {
    }

    /**
     * Fully validated, ready-to-delete cleanup event extracted from a Kafka message.
     */
    record CleanupEvent(
            String userId,
            String notificationId,
            Instant createdAt,
            String actionTable) {
    }

    /**
     * Per-run counts returned to {@link #runCleanup} for summary logging.
     */
    record CleanupSummary(int eligible, int deleted, int failed) {
    }

    public PeerValidationCleanupServiceImpl(CbServerProperties cbServerProperties,
                                            CassandraOperation cassandraOperation,
                                            ObjectMapper objectMapper) {
        this.cbServerProperties = cbServerProperties;
        this.cassandraOperation = cassandraOperation;
        this.objectMapper = objectMapper;
    }

    /**
     * Entry point for the cleanup job.
     *
     * <p>Derives the target date from {@code jobInstant} (UTC), then reads Kafka events
     * for that day in a fully pipelined fashion. Events are accumulated into chunks of
     * {@code cleanup.peer.validation.batch.size} and each chunk is processed in parallel
     * using a fixed thread pool of size {@code cleanup.peer.validation.thread.pool.size}.
     * The executor blocks ({@code invokeAll}) until a chunk is fully processed before the
     * next chunk is accepted, providing explicit back-pressure and bounding both memory
     * usage and the number of in-flight Cassandra operations.
     *
     * @param jobInstant reference instant supplied by the scheduler
     */
    @Override
    public void runCleanup(Instant jobInstant) {
        LocalDate targetDate = LocalDate.ofInstant(jobInstant, ZoneOffset.UTC).minusDays(cbServerProperties.getCleanupDayOffset());
        log.info("runCleanup: starting for date={} (jobInstant={})", targetDate, jobInstant);
        int chunkSize = cbServerProperties.getCleanupBatchSize();
        ExecutorService executor = Executors.newFixedThreadPool(cbServerProperties.getCleanupThreadPoolSize());
        LongAdder eventsRead = new LongAdder();
        LongAdder eligible = new LongAdder();
        LongAdder deleted = new LongAdder();
        LongAdder failed = new LongAdder();
        List<String> chunk = new ArrayList<>(chunkSize);
        try {
            readKafkaEventsForDay(targetDate, raw -> {
                eventsRead.increment();
                chunk.add(raw);
                if (chunk.size() >= chunkSize) {
                    processChunk(List.copyOf(chunk), targetDate, executor, eligible, deleted, failed);
                    chunk.clear();
                }
            });
            if (!chunk.isEmpty()) {
                processChunk(List.copyOf(chunk), targetDate, executor, eligible, deleted, failed);
            }
        } finally {
            shutdownExecutor(executor);
        }

        log.info("runCleanup: completed date={} eventsRead={} eligible={} deleted={} failed={}",
                targetDate, eventsRead.sum(), eligible.sum(), deleted.sum(), failed.sum());
    }

    /**
     * Shuts down the given executor and waits for task completion up to
     * {@code cleanup.peer.validation.executor.shutdown.timeout.minutes} minutes.
     *
     * @param executor the executor to shut down
     */
    private void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            int timeoutMinutes = cbServerProperties.getCleanupExecutorShutdownTimeoutMinutes();
            if (!executor.awaitTermination(timeoutMinutes, TimeUnit.MINUTES)) {
                log.warn("shutdownExecutor: did not terminate within {} minutes", timeoutMinutes);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("shutdownExecutor: interrupted while awaiting executor shutdown");
        }
    }

    /**
     * Submits a chunk of raw event strings to the executor as parallel tasks and blocks
     * until the entire chunk is processed ({@code invokeAll} semantics).
     *
     * <p>Each task deletes its row individually (one {@code deleteRecord} call) then
     * returns the built audit {@link Map} on success, or {@code null} on any failure.
     * After all tasks complete, non-null audit records are flushed together with a single
     * {@code insertBulkRecord} call — replacing N individual audit inserts with one.
     *
     * @param chunk      list of raw JSON event strings to process
     * @param targetDate the job date passed through to each delete/audit operation
     * @param executor   the fixed-size thread pool to submit tasks to
     * @param eligible   thread-safe counter for events that passed validation
     * @param deleted    thread-safe counter for successfully deleted rows
     * @param failed     thread-safe counter for validation or Cassandra failures
     */
    private void processChunk(List<String> chunk, LocalDate targetDate, ExecutorService executor,
                              LongAdder eligible, LongAdder deleted, LongAdder failed) {
        List<Callable<Map<String, Object>>> tasks = chunk.stream()
                .<Callable<Map<String, Object>>>map(
                        raw -> () -> executeDeleteTask(raw, targetDate, eligible, deleted, failed))
                .toList();

        List<Map<String, Object>> auditRecords = invokeAndCollectAuditMaps(tasks, executor);
        if (!auditRecords.isEmpty()) {
            bulkPersistAudit(auditRecords);
        }
    }

    /**
     * Validates and deletes a single raw event. Returns an audit map on success,
     * or {@code null} if validation fails or a Cassandra error occurs.
     *
     * @param raw        raw JSON event string
     * @param targetDate job date for the audit record
     * @param eligible   incremented when the event passes validation
     * @param deleted    incremented on successful row deletion
     * @param failed     incremented on validation failure or Cassandra error
     * @return the audit record map on success, or an empty map on validation/Cassandra failure
     */
    private Map<String, Object> executeDeleteTask(String raw, LocalDate targetDate,
                                                  LongAdder eligible, LongAdder deleted,
                                                  LongAdder failed) {
        Optional<CleanupEvent> event = parseAndValidate(raw);
        if (event.isEmpty()) {
            failed.increment();
            return Map.of();
        }
        CleanupEvent ce = event.get();
        eligible.increment();
        try {
            deleteActionRecord(ce);
            log.debug("executeDeleteTask: deleted notificationId={} table={}", ce.notificationId(), ce.actionTable());
            deleted.increment();
            return buildAuditRecord(ce, targetDate);
        } catch (Exception e) {
            log.error("executeDeleteTask: Cassandra delete failed notificationId={}: {}",
                    ce.notificationId(), e.getMessage(), e);
            failed.increment();
            return Map.of();
        }
    }

    /**
     * Submits tasks via {@code invokeAll} and collects non-empty results.
     * Returns an empty list if the calling thread is interrupted.
     *
     * @param tasks    callable tasks, each returning an audit map (empty map signals failure)
     * @param executor the executor to invoke tasks on
     * @return list of audit maps with at least one entry from successful tasks
     */
    private List<Map<String, Object>> invokeAndCollectAuditMaps(
            List<Callable<Map<String, Object>>> tasks, ExecutorService executor) {
        try {
            List<Future<Map<String, Object>>> futures = executor.invokeAll(tasks);
            return futures.stream()
                    .map(f -> {
                        try {
                            return f.get();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return Map.<String, Object>of();
                        } catch (Exception e) {
                            return Map.<String, Object>of();
                        }
                    })
                    .filter(m -> !m.isEmpty())
                    .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("invokeAndCollectAuditMaps: interrupted while waiting for chunk completion");
            return List.of();
        }
    }

    /**
     * Reads all Kafka messages from every configured cleanup topic whose broker
     * timestamp falls within [00:01:00, 23:59:59] UTC on {@code targetDate} and
     * passes each raw JSON value directly to {@code eventHandler} as it arrives,
     * so no full-day list is ever held in memory.
     *
     * @param targetDate   the day to scan
     * @param eventHandler called once per in-window message value
     */
    private void readKafkaEventsForDay(LocalDate targetDate, Consumer<String> eventHandler) {
        TimeWindow window = buildTimeWindow(targetDate);
        List<String> topics = resolveKafkaTopics();
        log.info("readKafkaEventsForDay: topics={}", topics);

        try (KafkaConsumer<String, String> consumer = createKafkaConsumer()) {
            for (String topic : topics) {
                LongAdder topicCount = new LongAdder();
                readTopic(consumer, topic, window, raw -> {
                    topicCount.increment();
                    eventHandler.accept(raw);
                });
                log.info("readKafkaEventsForDay: topic={} events={}", topic, topicCount.sum());
            }
        }
    }

    /**
     * Resolves the list of Kafka topics to scan.
     *
     * @return ordered list of non-blank topic names
     */
    private List<String> resolveKafkaTopics() {
        return parseTopics(cbServerProperties.getCleanupKafkaTopics());
    }

    /**
     * Builds the UTC epoch-ms time window for a given date.
     *
     * <p>The window starts at 00:01:00 (skipping midnight to avoid boundary edge cases)
     * and ends at 23:59:59 to cover the full working day.
     *
     * @param date the target calendar date
     * @return a {@link TimeWindow} with {@code startMs} and {@code endMs}
     */
    private TimeWindow buildTimeWindow(LocalDate date) {
        LocalTime windowStart = LocalTime.parse(cbServerProperties.getCleanupWindowStartTime());
        LocalTime windowEnd = LocalTime.parse(cbServerProperties.getCleanupWindowEndTime());
        long startMs = ZonedDateTime.of(date, windowStart, ZoneOffset.UTC).toInstant().toEpochMilli();
        long endMs = ZonedDateTime.of(date, windowEnd, ZoneOffset.UTC).toInstant().toEpochMilli();
        return new TimeWindow(startMs, endMs);
    }

    /**
     * Parses a comma-separated topic string into a trimmed, non-empty list.
     *
     * <p>Guards against trailing commas or extra whitespace in the properties value.
     *
     * @param raw comma-separated topic names from configuration
     * @return ordered list of non-blank topic names
     */
    private List<String> parseTopics(String raw) {
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(t -> !t.isBlank())
                .toList();
    }

    /**
     * Reads all messages from a single topic within the given time window,
     * forwarding each in-window value immediately to {@code eventHandler}.
     *
     * @param consumer     the Kafka consumer to use
     * @param topic        the topic name to scan
     * @param window       the epoch-ms time window
     * @param eventHandler called once per in-window message value
     */
    private void readTopic(KafkaConsumer<String, String> consumer, String topic, TimeWindow window,
                           Consumer<String> eventHandler) {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            log.warn("readTopic: no partitions found for topic={}", topic);
            return;
        }

        List<TopicPartition> allPartitions = toTopicPartitions(partitionInfos);
        consumer.assign(allPartitions);

        Set<TopicPartition> activePartitions = resolveActivePartitions(consumer, allPartitions, window.startMs());
        if (activePartitions.isEmpty()) {
            return;
        }

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(allPartitions);
        collectWindowedRecords(consumer, activePartitions, endOffsets, window, eventHandler);
    }

    /**
     * Converts a list of {@link PartitionInfo} to a list of {@link TopicPartition}.
     *
     * @param partitionInfos partition metadata from the broker
     * @return topic-partition identifiers used by the consumer API
     */
    private List<TopicPartition> toTopicPartitions(List<PartitionInfo> partitionInfos) {
        return partitionInfos.stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
    }

    /**
     * For each partition, seeks to the first offset at or after {@code startMs}.
     * Partitions with no data at or after {@code startMs}, or already at log-end, are excluded.
     *
     * @param consumer      the Kafka consumer
     * @param allPartitions all partitions of the topic
     * @param startMs       the epoch-ms lower bound
     * @return partitions that have been seeked and are ready for polling
     */
    private Set<TopicPartition> resolveActivePartitions(KafkaConsumer<String, String> consumer,
                                                        List<TopicPartition> allPartitions,
                                                        long startMs) {
        Map<TopicPartition, Long> timestampQuery = new HashMap<>();
        allPartitions.forEach(tp -> timestampQuery.put(tp, startMs));

        Map<TopicPartition, OffsetAndTimestamp> startOffsets = consumer.offsetsForTimes(timestampQuery);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(allPartitions);

        Set<TopicPartition> active = new HashSet<>();
        for (TopicPartition tp : allPartitions) {
            OffsetAndTimestamp ots = startOffsets.get(tp);
            Long endOffset = endOffsets.get(tp);
            if (ots != null && endOffset != null && endOffset > ots.offset()) {
                consumer.seek(tp, ots.offset());
                active.add(tp);
            }
        }
        return active;
    }

    /**
     * Polls the consumer in a loop, forwarding each in-window message value to
     * {@code eventHandler} immediately — no intermediate list is created.
     * A partition is marked exhausted when a record's timestamp exceeds
     * {@code endMs} or the record reaches the pre-fetched log-end offset.
     *
     * @param consumer         the Kafka consumer
     * @param activePartitions mutable set of partitions still being drained
     * @param endOffsets       pre-fetched log-end offsets per partition
     * @param window           the epoch-ms time window
     * @param eventHandler     called once per in-window message value
     */
    private void collectWindowedRecords(KafkaConsumer<String, String> consumer,
                                        Set<TopicPartition> activePartitions,
                                        Map<TopicPartition, Long> endOffsets,
                                        TimeWindow window,
                                        Consumer<String> eventHandler) {
        while (!activePartitions.isEmpty()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(cbServerProperties.getCleanupPollTimeoutSeconds()));
            if (records.isEmpty()) {
                break;
            }
            Set<TopicPartition> exhausted = new HashSet<>();
            for (ConsumerRecord<String, String> kafkaRecord : records) {
                processRecord(kafkaRecord, endOffsets, window, eventHandler, exhausted);
            }
            activePartitions.removeAll(exhausted);
        }
    }

    /**
     * Evaluates a single Kafka record against the time window.
     *
     * <p>Invokes {@code eventHandler} with the message value if the timestamp is within
     * bounds, and marks the partition as exhausted when the record surpasses the window
     * end or reaches the log-end offset.
     *
     * @param kafkaRecord  the Kafka record to evaluate
     * @param endOffsets   pre-fetched log-end offsets per partition
     * @param window       the epoch-ms time window
     * @param eventHandler called with the message value when it falls within the window
     * @param exhausted    accumulator for partitions that should stop being polled
     */
    private void processRecord(ConsumerRecord<String, String> kafkaRecord,
                               Map<TopicPartition, Long> endOffsets,
                               TimeWindow window,
                               Consumer<String> eventHandler,
                               Set<TopicPartition> exhausted) {
        TopicPartition tp = new TopicPartition(kafkaRecord.topic(), kafkaRecord.partition());

        if (kafkaRecord.timestamp() > window.endMs()) {
            exhausted.add(tp);
            return;
        }

        if (kafkaRecord.timestamp() >= window.startMs()) {
            eventHandler.accept(kafkaRecord.value());
        }

        Long endOffset = endOffsets.get(tp);
        if (endOffset != null && kafkaRecord.offset() >= endOffset - 1) {
            exhausted.add(tp);
        }
    }

    /**
     * Iterates over raw Kafka event strings, parses and validates each one, and deletes
     * the corresponding Cassandra rows. Validation and delete failures are only logged;
     * successful deletions are written to the audit table.
     *
     * <p>Package-private to allow direct unit-test access without a full Kafka mock setup.
     *
     * @param rawEvents  list of raw JSON strings from Kafka
     * @param targetDate the cleanup job date used in audit records
     * @return a {@link CleanupSummary} with counts of eligible, deleted, and failed rows
     */
    CleanupSummary processCleanupEvents(List<String> rawEvents, LocalDate targetDate) {
        int eligible = 0;
        int deleted = 0;
        int failed = 0;
        List<Map<String, Object>> auditRecords = new ArrayList<>();
        for (String raw : rawEvents) {
            Optional<CleanupEvent> event = parseAndValidate(raw);
            if (event.isEmpty()) {
                failed++;
                continue;
            }
            CleanupEvent ce = event.get();
            eligible++;
            try {
                deleteActionRecord(ce);
                auditRecords.add(buildAuditRecord(ce, targetDate));
                deleted++;
            } catch (Exception e) {
                log.error("processCleanupEvents: Cassandra delete failed notificationId={}: {}",
                        ce.notificationId(), e.getMessage(), e);
                failed++;
            }
        }
        if (!auditRecords.isEmpty()) {
            bulkPersistAudit(auditRecords);
        }
        return new CleanupSummary(eligible, deleted, failed);
    }

    /**
     * Parses and fully validates a raw JSON event string into a {@link CleanupEvent}.
     * Returns {@link Optional#empty()} if parsing or any validation step fails.
     *
     * @param raw the raw JSON event string
     * @return an {@link Optional} containing the validated event, or empty
     */
    private Optional<CleanupEvent> parseAndValidate(String raw) {
        Map<String, Object> eventMap = parseEventJson(raw);
        if (eventMap.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(extractCleanupEvent(eventMap));
    }

    /**
     * Parses a raw JSON string into a map.
     * Returns {@code null} and logs a warning if parsing fails.
     *
     * @param raw the raw JSON string
     * @return the parsed map, or an empty map if parsing fails
     */
    private Map<String, Object> parseEventJson(String raw) {
        try {
            return objectMapper.readValue(raw, EVENT_TYPE_REF);
        } catch (Exception e) {
            log.warn("parseEventJson: JSON parse error: {}", e.getMessage());
            return Map.of();
        }
    }

    /**
     * Extracts and validates all required fields from a parsed event map, then builds a
     * {@link CleanupEvent}. Returns {@code null} and logs a warning if any field is
     * missing, blank, or unrecognised.
     *
     * @param event the parsed event map
     * @return a validated {@link CleanupEvent}, or {@code null} on validation failure
     */
    private CleanupEvent extractCleanupEvent(Map<String, Object> event) {
        String userId = asString(event, Constants.USER_ID_FIELD);
        String notificationId = asString(event, Constants.NOTIFICATION_ID_FIELD);
        String createdAtStr = asString(event, Constants.CREATED_AT_FIELD);
        String subCategory = asString(event, Constants.SUB_CATEGORY_FIELD);

        if (StringUtils.isAnyBlank(userId, notificationId, createdAtStr, subCategory)) {
            log.warn("extractCleanupEvent: missing or blank required fields in event");
            return null;
        }

        String actionTable = resolveActionTable(subCategory);
        if (actionTable == null) {
            log.warn("extractCleanupEvent: unrecognised subCategory='{}' notificationId={}", subCategory, notificationId);
            return null;
        }

        if (isReviewTable(actionTable) && !isReviewStatusEligible(event, notificationId)) {
            return null;
        }

        Instant createdAt = parseCreatedAt(createdAtStr, notificationId);
        if (createdAt == null) {
            return null;
        }
        return new CleanupEvent(userId, notificationId, createdAt, actionTable);
    }

    /**
     * Returns {@code true} if the given table name is the peer-validation reviews table.
     *
     * @param actionTable the resolved Cassandra table name
     * @return {@code true} for the reviews table, {@code false} otherwise
     */
    private boolean isReviewTable(String actionTable) {
        return cbServerProperties.getCleanupTableReviews().equals(actionTable);
    }

    /**
     * Checks whether the {@code status} field of a PEER_REVIEW_ASSIGNED event is in the
     * configured list of statuses eligible for deletion.
     *
     * <p>PEER_EVALUATION_ASSIGNED events have no status field and therefore bypass this check.
     *
     * @param event          the parsed event map
     * @param notificationId used in the warning log if the status is ineligible
     * @return {@code true} if the status passes the eligibility check
     */
    private boolean isReviewStatusEligible(Map<String, Object> event, String notificationId) {
        String status = asString(event, Constants.STATUS_FIELD);
        List<String> allowedStatuses = cbServerProperties.getPeerReviewAssignedExcludedStatuses();
        boolean eligible = status != null && allowedStatuses.stream().anyMatch(status::equalsIgnoreCase);
        if (!eligible) {
            log.warn("isReviewStatusEligible: status='{}' is not eligible for deletion notificationId={}",
                    status, notificationId);
        }
        return eligible;
    }

    /**
     * Parses the ISO-8601 {@code createdAt} string to an {@link Instant}.
     * Returns {@code null} and logs a warning on parse failure.
     *
     * @param createdAtStr   the raw timestamp string
     * @param notificationId used in the warning log if parsing fails
     * @return the parsed {@link Instant}, or {@code null}
     */
    private Instant parseCreatedAt(String createdAtStr, String notificationId) {
        try {
            return Instant.parse(createdAtStr);
        } catch (DateTimeParseException e) {
            log.warn("parseCreatedAt: invalid createdAt='{}' notificationId={}", createdAtStr, notificationId);
            return null;
        }
    }

    /**
     * Builds an audit record map for a successfully deleted event.
     * The map is collected across all events in a chunk and flushed in bulk via
     * {@link #bulkPersistAudit}.
     *
     * @param event   the event that was successfully deleted
     * @param jobDate the cleanup job date for correlation
     * @return the audit record ready for bulk insertion
     */
    private Map<String, Object> buildAuditRecord(CleanupEvent event, LocalDate jobDate) {
        Map<String, Object> audit = new HashMap<>();
        audit.put(Constants.USER_ID, event.userId());
        audit.put(Constants.NOTIFICATION_ID, event.notificationId());
        audit.put(Constants.FAILURE_REASON, cbServerProperties.getCleanupAuditDeletionPrefix() + event.actionTable());
        audit.put(Constants.JOB_DATE, jobDate.toString());
        return audit;
    }

    /**
     * Inserts a batch of audit records into the cleanup audit table using a single
     * {@code insertBulkRecord} call, replacing N individual inserts with one round-trip.
     *
     * <p>Any exception is swallowed with a WARN log so that an audit failure never
     * interrupts the main cleanup loop.
     *
     * @param auditRecords non-empty list of audit maps to insert
     */
    private void bulkPersistAudit(List<Map<String, Object>> auditRecords) {
        try {
            String auditTable = resolveAuditTable();
            cassandraOperation.insertBulkRecord(Constants.KEYSPACE_SUNBIRD, auditTable, auditRecords);
            log.info("bulkPersistAudit: inserted {} audit records into {}", auditRecords.size(), auditTable);
        } catch (Exception ex) {
            log.warn("bulkPersistAudit: could not insert {} audit records: {}",
                    auditRecords.size(), ex.getMessage());
        }
    }

    /**
     * Returns the audit table name from configuration.
     *
     * @return the audit table name
     */
    private String resolveAuditTable() {
        return cbServerProperties.getCleanupTableAudit();
    }

    /**
     * Deletes the action-table row using the {@code (user_id, notification_id)} primary key.
     *
     * @param event the cleanup event whose action row should be removed
     */
    private void deleteActionRecord(CleanupEvent event) {
        cassandraOperation.deleteRecord(
                Constants.KEYSPACE_SUNBIRD,
                event.actionTable(),
                Map.of(Constants.USER_ID, event.userId(), Constants.NOTIFICATION_ID, event.notificationId())
        );
    }

    /**
     * Maps a {@code subCategory} value to the corresponding Cassandra action table name.
     * Uses a Java 17 switch expression for exhaustive, readable dispatch.
     *
     * @param subCategory the sub-category from the Kafka event (case-insensitive)
     * @return the table name, or {@code null} for an unrecognised sub-category
     */
    private String resolveActionTable(String subCategory) {
        if (subCategory == null) {
            return null;
        }
        return switch (subCategory.toUpperCase(java.util.Locale.ROOT)) {
            case Constants.SUB_CATEGORY_PEER_EVALUATION_ASSIGNED -> cbServerProperties.getCleanupTableRequests();
            case Constants.SUB_CATEGORY_PEER_REVIEW_ASSIGNED -> cbServerProperties.getCleanupTableReviews();
            default -> null;
        };
    }

    /**
     * Retrieves a field from a map as a {@code String} using Java 17 pattern matching.
     * Returns {@code null} for absent or non-string values.
     *
     * @param map the event map
     * @param key the field key
     * @return the string value, or {@code null}
     */
    private static String asString(Map<String, Object> map, String key) {
        return map.get(key) instanceof String s ? s : null;
    }

    /**
     * Creates a standalone, manually-assigned {@link KafkaConsumer} for the cleanup job.
     *
     * <p>Auto-commit is disabled because offsets are never committed — the consumer is
     * used purely for a one-shot time-windowed scan and is closed immediately after.
     * Package-private to allow unit tests to substitute a mock via a Mockito spy.
     *
     * @return a configured, ready-to-use {@link KafkaConsumer}
     */
    KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cbServerProperties.getSpringKafkaBootStrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, cbServerProperties.getCleanupConsumerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(cbServerProperties.getCleanupBatchSize()));
        return new KafkaConsumer<>(props);
    }
}
