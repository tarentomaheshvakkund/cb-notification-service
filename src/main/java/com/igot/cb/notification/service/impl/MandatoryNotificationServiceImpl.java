package com.igot.cb.notification.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.notification.enums.NotificationReadStatus;
import com.igot.cb.notification.enums.NotificationSubType;
import com.igot.cb.notification.service.MandatoryNotificationService;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.ApiResponse;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

import static com.igot.cb.util.Constants.*;

@Service
@Slf4j
public class MandatoryNotificationServiceImpl implements MandatoryNotificationService {

    private final AccessTokenValidator accessTokenValidator;
    private final CassandraOperation cassandraOperation;
    private final ObjectMapper objectMapper;
    private final CbServerProperties cbServerProperties;

    public MandatoryNotificationServiceImpl(AccessTokenValidator accessTokenValidator,
                                            CassandraOperation cassandraOperation,
                                            ObjectMapper objectMapper,
                                            CbServerProperties cbServerProperties) {
        this.accessTokenValidator = accessTokenValidator;
        this.cassandraOperation = cassandraOperation;
        this.objectMapper = objectMapper;
        this.cbServerProperties = cbServerProperties;
    }

    /**
     * Retrieves a paginated list of mandatory notifications for the authenticated user.
     * Filters by date range, read status, and sub-type. Also computes sub-type statistics.
     *
     * @param token   auth token to identify the user
     * @param days    number of past days to include
     * @param page    zero-based page index
     * @param size    page size
     * @param status  read/unread/both filter
     * @param subType optional sub-type filter (e.g. ALERT, UPDATE)
     * @return ApiResponse containing paginated notifications and sub-type stats
     */
    @Override
    public ApiResponse getMandatoryNotificationsList(String token, int days, int page, int size, NotificationReadStatus status, String subType) {
        log.info("getMandatoryNotificationsList: started");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.USER_MANDATORY_NOTIFICATION_LIST);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
            if (StringUtils.isEmpty(userId)) {
                log.warn("getMandatoryNotificationsList: Invalid or missing auth token");
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.BAD_REQUEST);
                return response;
            }
            Instant fromDate = ZonedDateTime.now(ZoneOffset.UTC).minusDays(days).toInstant();
            List<Map<String, Object>> merged = fetchAndMergeNotifications(userId);
            List<Map<String, Object>> sortedEligible = merged.stream()
                    .filter(n -> isNotificationEligible(n, fromDate, status))
                    .sorted(Comparator.comparing(n -> (Instant) n.get(CREATED_AT), Comparator.nullsLast(Comparator.reverseOrder())))
                    .toList();
            response.setResponseCode(HttpStatus.OK);
            response.setResult(buildPaginatedResult(sortedEligible, subType, page, size));
            log.info("getMandatoryNotificationsList: completed, count={}", sortedEligible.size());
        } catch (Exception e) {
            log.error("getMandatoryNotificationsList: Unexpected error - {}", e.getMessage(), e);
            updateErrorDetails(response, ERR_FETCHING_NOTIFICATION_LIST, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return response;
    }

    /**
     * Fetches notification records for the given user from the mandatory_notification table.
     *
     * @param userId the user's unique identifier
     * @return list of raw notification records from Cassandra
     */
    private List<Map<String, Object>> fetchAndMergeNotifications(String userId) {
        List<String> fields = List.of(NOTIFICATION_ID, CREATED_AT, TYPE, MESSAGE, READ, ROLE, SOURCE, CATEGORY, SUB_CATEGORY, SUB_TYPE, IS_DELETED);
        List<Map<String, Object>> records = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_MANDATORY_NOTIFICATION,
                Map.of(USER_ID, userId), fields, cbServerProperties.getMandatoryNotificationMaxFetchLimit()
        );
        records.forEach(n -> {
            Object fetchedDate = n.get(CREATED_AT);
            if (!(fetchedDate instanceof Instant)) {
                n.put(CREATED_AT, getInstant(fetchedDate));
            }
        });
        return records;
    }

    /**
     * Checks whether a single notification passes the date, deletion, and read-status criteria.
     */
    private boolean isNotificationEligible(Map<String, Object> n, Instant fromDate, NotificationReadStatus status) {
        Instant createdAt = (Instant) n.get(CREATED_AT);
        if (createdAt == null || createdAt.isBefore(fromDate)) return false;
        if (Boolean.TRUE.equals(n.get(IS_DELETED))) return false;
        Boolean isRead = (Boolean) n.get(READ);
        if (status == NotificationReadStatus.READ && !Boolean.TRUE.equals(isRead)) return false;
        return status != NotificationReadStatus.UNREAD || Boolean.FALSE.equals(isRead);
    }

    /**
     * Builds per-sub-type read/unread statistics from the provided notifications.
     * Results are ordered by the fixed {@link NotificationSubType} ordinal.
     *
     * @param notifications filtered and sorted notification list
     * @return list of maps, each containing name, read count, and unread count
     */
    private List<Map<String, Object>> buildSubTypeStats(List<Map<String, Object>> notifications) {
        return notifications.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        n -> (String) n.getOrDefault(SUB_TYPE, ALL),
                        java.util.stream.Collectors.teeing(
                                java.util.stream.Collectors.summingInt(n -> Boolean.TRUE.equals(n.get(READ)) ? 1 : 0),
                                java.util.stream.Collectors.summingInt(n -> Boolean.FALSE.equals(n.get(READ)) ? 1 : 0),
                                (readCount, unreadCount) -> Map.of(READ, (Object) readCount, UNREAD, (Object) unreadCount)
                        )
                ))
                .entrySet().stream()
                .map(e -> {
                    Map<String, Object> stat = new HashMap<>();
                    stat.put(NAME, e.getKey());
                    stat.putAll(e.getValue());
                    return stat;
                })
                .sorted(Comparator.comparingInt(stat -> getFixedOrderIndex((String) stat.get(NAME))))
                .toList();
    }

    /**
     * Builds a paginated result map containing the current page of notifications,
     * pagination metadata, and sub-type statistics. Computes stats and applies
     * optional sub-type filter inline to minimize intermediate list allocations.
     *
     * @param notifications full sorted and filtered list
     * @param subType       optional sub-type filter (blank means all)
     * @param page          zero-based page index
     * @param size          page size
     * @return result map ready to set on the ApiResponse
     */
    private Map<String, Object> buildPaginatedResult(List<Map<String, Object>> notifications, String subType, int page, int size) {
        List<Map<String, Object>> subTypeStats = buildSubTypeStats(notifications);

        List<Map<String, Object>> source = StringUtils.isBlank(subType)
                ? notifications
                : notifications.stream()
                        .filter(n -> subType.equalsIgnoreCase((String) n.getOrDefault(SUB_TYPE, ALL)))
                        .toList();

        int total = source.size();
        List<Map<String, Object>> processed = source.stream()
                .skip((long) page * size)
                .limit(size)
                .map(this::prepareNotificationResponse)
                .toList();

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(NOTIFICATIONS, processed);
        resultMap.put(TOTAL_COUNT, total);
        resultMap.put(PAGE, page);
        resultMap.put(SIZE, size);
        resultMap.put(HAS_NEXT_PAGE, (long) (page + 1) * size < total);
        resultMap.put(SUBTYPE_STATS, subTypeStats);
        return resultMap;
    }

    /**
     * Retrieves the oldest unread mandatory notification for the authenticated user.
     *
     * @param token auth token to identify the user
     * @return ApiResponse containing a single notification or an empty map if none exist
     */
    @Override
    public ApiResponse getCurrentMandatoryNotification(String token) {
        log.info("getCurrentMandatoryNotification: started");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.USER_MANDATORY_NOTIFICATION_CURRENT);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
            if (StringUtils.isEmpty(userId)) {
                log.warn("getCurrentMandatoryNotification: Invalid or missing auth token");
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.BAD_REQUEST);
                return response;
            }
            List<Map<String, Object>> merged = fetchAndMergeNotifications(userId);
            Optional<Map<String, Object>> oldestNotification = merged.stream()
                    .filter(n -> isNotificationEligible(n, Instant.MIN, NotificationReadStatus.UNREAD))
                    .min(Comparator.comparing(n -> (Instant) n.get(CREATED_AT), Comparator.nullsLast(Comparator.naturalOrder())));
            response.setResponseCode(HttpStatus.OK);
            if (oldestNotification.isPresent()) {
                Map<String, Object> notification = prepareNotificationResponse(oldestNotification.get());
                response.setResult(Map.of(NOTIFICATION, notification));
            } else {
                response.setResult(Map.of(NOTIFICATION, Collections.emptyMap()));
            }
            log.info("getCurrentMandatoryNotification: completed, found={}", oldestNotification.isPresent());
        } catch (Exception e) {
            log.error("getCurrentMandatoryNotification: Unexpected error - {}", e.getMessage(), e);
            updateErrorDetails(response, ERR_FETCHING_NOTIFICATION, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Marks a single mandatory notification as read using the composite key (user_id, created_at).
     * The request body must contain both 'id' and 'createdAt' fields.
     *
     * @param token       auth token to identify the user
     * @param requestBody request map containing 'request' with 'id' and 'createdAt'
     * @return ApiResponse indicating success or failure
     */
    @Override
    public ApiResponse markMandatoryNotificationsAsRead(String token, Map<String, Object> requestBody) {
        log.info("markMandatoryNotificationsAsRead: started");
        Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.USER_MANDATORY_NOTIFICATION_READ);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
        if (StringUtils.isEmpty(userId)) {
            log.warn("markMandatoryNotificationsAsRead: Invalid or missing auth token");
            updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.BAD_REQUEST);
            return response;
        }
        String notificationId = (String) request.get(ID);
        String createdAtStr = (String) request.get(CREATED_AT);
        if (StringUtils.isBlank(notificationId) || StringUtils.isBlank(createdAtStr)) {
            log.warn("markMandatoryNotificationsAsRead: Missing required fields - id={}, createdAt={}", notificationId, createdAtStr);
            updateErrorDetails(response, ERR_ID_AND_CREATED_AT_REQUIRED, HttpStatus.BAD_REQUEST);
            return response;
        }
        try {
            Instant createdAt = getInstant(createdAtStr);
            if (createdAt == null) {
                log.warn("markMandatoryNotificationsAsRead: Invalid createdAt format - {}", createdAtStr);
                updateErrorDetails(response, ERR_INVALID_CREATED_AT_FORMAT, HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> compositeKey = Map.of(USER_ID, userId, CREATED_AT, createdAt);
            List<Map<String, Object>> existing = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_MANDATORY_NOTIFICATION,
                    compositeKey, Collections.singletonList(NOTIFICATION_ID), 1
            );
            if (CollectionUtils.isEmpty(existing)) {
                log.warn("markMandatoryNotificationsAsRead: Notification not found for userId={}, createdAt={}", userId, createdAt);
                updateErrorDetails(response, ERR_NOTIFICATION_NOT_FOUND, HttpStatus.NOT_FOUND);
                return response;
            }
            Instant now = Instant.now();
            Map<String, Object> result = cassandraOperation.updateRecordByCompositeKey(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_MANDATORY_NOTIFICATION,
                    Map.of(READ, true, READ_AT, now),
                    compositeKey
            );
            if (Constants.SUCCESS.equalsIgnoreCase((String) result.get(Constants.RESPONSE))) {
                response.getParams().setStatus(Constants.SUCCESS);
                response.getParams().setErrMsg(MSG_NOTIFICATION_MARKED_READ);
                response.setResponseCode(HttpStatus.OK);
                response.setResult(Map.of(ID, notificationId, READ, true, READ_AT, now.toString()));
                log.info("markMandatoryNotificationsAsRead: completed");
                decrementUnreadCount(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_UNREAD_NOTIFICATION_COUNT, userId);
            } else {
                log.error("markMandatoryNotificationsAsRead: Cassandra update failed for notificationId={}, userId={}", notificationId, userId);
                updateErrorDetails(response, ERR_FAILED_TO_UPDATE_NOTIFICATION, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception e) {
            log.error("markMandatoryNotificationsAsRead: Unexpected error - {}", e.getMessage(), e);
            updateErrorDetails(response, ERR_UPDATING_NOTIFICATION, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return response;
    }

    /**
     * Transforms a raw DB record into a client-facing notification map.
     * Removes internal fields (is_deleted, updated_at, user_id, read_at, template_id),
     * parses the message field from JSON string to JsonNode, and formats created_at as ISO string.
     */
    private Map<String, Object> prepareNotificationResponse(Map<String, Object> dbRecord) {
        Map<String, Object> resultMap = new HashMap<>(dbRecord);
        List.of(Constants.IS_DELETED, Constants.UPDATED_AT, Constants.USER_ID, Constants.READ_AT, Constants.TEMPLATE_ID)
                .forEach(resultMap::remove);
        Object messageObj = resultMap.get(Constants.MESSAGE);
        if (messageObj instanceof String strMessage) {
            try {
                JsonNode parsed = objectMapper.readTree(strMessage);
                resultMap.put(Constants.MESSAGE, parsed);
            } catch (Exception e) {
                log.warn("prepareNotificationResponse: Failed to parse message as JSON - {}", e.getMessage());
            }
        }
        Object createdAtObj = resultMap.get(Constants.CREATED_AT);
        if (createdAtObj instanceof Instant instant) {
            resultMap.put(Constants.CREATED_AT, instant.toString());
        }
        return resultMap;
    }

    /**
     * Returns the fixed display order index for a given sub-type based on {@link NotificationSubType} ordinal.
     * Unknown sub-types are placed at the end.
     */
    private int getFixedOrderIndex(String subType) {
        try {
            if (subType != null) {
                return NotificationSubType.valueOf(subType.toUpperCase()).ordinal();
            }
        } catch (IllegalArgumentException e) {
            return Integer.MAX_VALUE;
        }
        return 0;
    }

    /**
     * Converts various possible date representations (Instant, Date, ISO-8601 String) to an Instant.
     *
     * @param value the raw value from the database or request
     * @return parsed Instant, or null if the value is unrecognized or unparseable
     */
    private Instant getInstant(Object value) {
        if (value instanceof Instant instant) {
            return instant;
        } else if (value instanceof Date date) {
            return date.toInstant();
        } else if (value instanceof String str) {
            try {
                return Instant.parse(str);
            } catch (Exception e) {
                log.warn("getInstant: Unparseable value - {}", value);
            }
        }
        return null;
    }

    /**
     * Sets error status, message, and HTTP status code on the given ApiResponse.
     */
    private void updateErrorDetails(ApiResponse response, String errorMessage, HttpStatus httpStatus) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrMsg(errorMessage);
        response.setResponseCode(httpStatus);
    }

    /**
     * Decrements the unread notification count for the given user by 1.
     * Count is never decremented below 0. If no record exists, the call is a no-op.
     */
    private void decrementUnreadCount(String keyspace, String table, String userId) {
        try {
            Map<String, Object> criteria = Map.of(USER_ID, userId);
            List<String> fields = Collections.singletonList(COUNT);
            List<Map<String, Object>> records = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    keyspace, table, criteria, fields, 1
            );
            if (CollectionUtils.isEmpty(records) || records.get(0).get(COUNT) == null) {
                log.warn("decrementUnreadCount: No count record found for user {} — skipping", userId);
                return;
            }
            int currentCount = (int) records.get(0).get(COUNT);
            if (currentCount <= 0) {
                log.warn("decrementUnreadCount: Count is already 0 for user {} — skipping", userId);
                return;
            }
            Map<String, Object> updates = new HashMap<>();
            updates.put(COUNT, currentCount - 1);
            updates.put(UPDATED_AT, Instant.now());
            cassandraOperation.updateRecordByCompositeKey(keyspace, table, updates, criteria);
        } catch (Exception e) {
            log.error("decrementUnreadCount: Failed for user={} error={}", userId, e.getMessage(), e);
        }
    }
}
