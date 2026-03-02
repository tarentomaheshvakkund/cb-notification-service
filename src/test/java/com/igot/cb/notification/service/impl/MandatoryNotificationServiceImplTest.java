package com.igot.cb.notification.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.notification.enums.NotificationReadStatus;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.ApiResponse;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.*;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Stream;

import static com.igot.cb.util.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class MandatoryNotificationServiceImplTest {

    @InjectMocks
    private MandatoryNotificationServiceImpl service;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private CbServerProperties cbServerProperties;

    private static final String AUTH_TOKEN = "test-auth-token";
    private static final String USER_ID_VAL = "user-123";

    private AutoCloseable closeable;

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        when(cbServerProperties.getMandatoryNotificationMaxFetchLimit()).thenReturn(100);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeable.close();
    }

    private Map<String, Object> buildNotification(String id, Instant createdAt, String subType,
                                                  boolean read, boolean isDeleted) {
        Map<String, Object> n = new HashMap<>();
        n.put(NOTIFICATION_ID, id);
        n.put(CREATED_AT, createdAt);
        n.put(SUB_TYPE, subType);
        n.put(READ, read);
        n.put(IS_DELETED, isDeleted);
        n.put(TYPE, "MANDATORY");
        n.put(MESSAGE, "{\"title\":\"Test\",\"body\":\"Test body\"}");
        n.put(ROLE, "ALL");
        n.put(SOURCE, "SYSTEM");
        n.put(CATEGORY, "GENERAL");
        n.put(SUB_CATEGORY, "INFO");
        return n;
    }

    private void mockValidUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(AUTH_TOKEN)).thenReturn(USER_ID_VAL);
    }

    private void mockInvalidUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(AUTH_TOKEN)).thenReturn(null);
    }

    private void mockEmptyUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(AUTH_TOKEN)).thenReturn("");
    }

    private void mockCassandraReturn(List<Map<String, Object>> records) {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_MANDATORY_NOTIFICATION),
                anyMap(), anyList(), anyInt()
        )).thenReturn(records);
    }

    private void mockObjectMapperReadTree() throws Exception {
        when(objectMapper.readTree(anyString())).thenAnswer(invocation -> {
            String json = invocation.getArgument(0, String.class);
            return new ObjectMapper().readTree(json);
        });
    }

    @Nested
    @DisplayName("getMandatoryNotificationsList")
    class GetMandatoryNotificationsListTests {
        @Test
        @DisplayName("should return BAD_REQUEST when auth token is invalid")
        void invalidToken_returnsBadRequest() {
            mockInvalidUser();
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals(Constants.FAILED, response.getParams().getStatus());
            assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrMsg());
        }

        @Test
        @DisplayName("should return BAD_REQUEST when auth token returns empty userId")
        void emptyUserId_returnsBadRequest() {
            mockEmptyUser();
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        }

        @Test
        @DisplayName("should return empty list when no notifications exist")
        void noNotifications_returnsEmptyList() throws Exception {
            mockValidUser();
            mockCassandraReturn(new ArrayList<>());
            mockObjectMapperReadTree();
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            assertNotNull(result);
            assertEquals(0, result.get(TOTAL_COUNT));
            assertTrue(((List<?>) result.get(NOTIFICATIONS)).isEmpty());
            assertFalse((Boolean) result.get(HAS_NEXT_PAGE));
        }

        @Test
        @DisplayName("should return paginated notifications sorted by createdAt descending")
        void multipleNotifications_returnsSortedDescending() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false),
                    buildNotification("n2", now.minus(3, ChronoUnit.HOURS), "UPDATE", true, false),
                    buildNotification("n3", now.minus(2, ChronoUnit.HOURS), "ALERT", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            List<Map<String, Object>> notifications = (List<Map<String, Object>>) result.get(NOTIFICATIONS);
            assertEquals(3, notifications.size());
            assertEquals("n1", notifications.get(0).get(NOTIFICATION_ID));
            assertEquals("n3", notifications.get(1).get(NOTIFICATION_ID));
            assertEquals("n2", notifications.get(2).get(NOTIFICATION_ID));
        }

        @Test
        @DisplayName("should filter by UNREAD status")
        void filterByUnread_returnsOnlyUnread() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false),
                    buildNotification("n2", now.minus(2, ChronoUnit.HOURS), "UPDATE", true, false),
                    buildNotification("n3", now.minus(3, ChronoUnit.HOURS), "ALERT", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.UNREAD, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            assertEquals(2, result.get(TOTAL_COUNT));
        }

        @Test
        @DisplayName("should filter by READ status")
        void filterByRead_returnsOnlyRead() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false),
                    buildNotification("n2", now.minus(2, ChronoUnit.HOURS), "UPDATE", true, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.READ, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            assertEquals(1, result.get(TOTAL_COUNT));
        }

        @Test
        @DisplayName("should exclude deleted notifications")
        void deletedNotifications_areExcluded() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false),
                    buildNotification("n2", now.minus(2, ChronoUnit.HOURS), "UPDATE", false, true)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            assertEquals(1, result.get(TOTAL_COUNT));
        }

        @Test
        @DisplayName("should exclude notifications older than the date range")
        void oldNotifications_areExcluded() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false),
                    buildNotification("n2", now.minus(60, ChronoUnit.DAYS), "UPDATE", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);

            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            assertEquals(1, result.get(TOTAL_COUNT));
        }

        @Test
        @DisplayName("should filter by sub_type when provided")
        void filterBySubType_returnsOnlyMatchingSubType() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false),
                    buildNotification("n2", now.minus(2, ChronoUnit.HOURS), "UPDATE", false, false),
                    buildNotification("n3", now.minus(3, ChronoUnit.HOURS), "ALERT", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, "ALERT");
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            assertEquals(2, result.get(TOTAL_COUNT));
        }

        @Test
        @DisplayName("should paginate correctly - first page")
        void pagination_firstPage() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                records.add(buildNotification("n" + i, now.minus(i, ChronoUnit.HOURS), "ALERT", false, false));
            }
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 2, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            List<Map<String, Object>> notifications = (List<Map<String, Object>>) result.get(NOTIFICATIONS);
            assertEquals(2, notifications.size());
            assertEquals(5, result.get(TOTAL_COUNT));
            assertTrue((Boolean) result.get(HAS_NEXT_PAGE));
            assertEquals(0, result.get(PAGE));
            assertEquals(2, result.get(SIZE));
        }

        @Test
        @DisplayName("should paginate correctly - last page")
        void pagination_lastPage() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                records.add(buildNotification("n" + i, now.minus(i, ChronoUnit.HOURS), "ALERT", false, false));
            }
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 2, 2, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            List<Map<String, Object>> notifications = (List<Map<String, Object>>) result.get(NOTIFICATIONS);
            assertEquals(1, notifications.size());
            assertFalse((Boolean) result.get(HAS_NEXT_PAGE));
        }

        @Test
        @DisplayName("should paginate correctly - page beyond data returns empty")
        void pagination_beyondData() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 5, 10, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            List<Map<String, Object>> notifications = (List<Map<String, Object>>) result.get(NOTIFICATIONS);
            assertTrue(notifications.isEmpty());
            assertFalse((Boolean) result.get(HAS_NEXT_PAGE));
        }

        @Test
        @DisplayName("should build subtypeStats with read/unread counts")
        void subtypeStats_containsReadUnreadCounts() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false),
                    buildNotification("n2", now.minus(2, ChronoUnit.HOURS), "ALERT", true, false),
                    buildNotification("n3", now.minus(3, ChronoUnit.HOURS), "UPDATE", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            Map<String, Object> result = response.getResult();
            List<Map<String, Object>> stats = (List<Map<String, Object>>) result.get(SUBTYPE_STATS);
            assertNotNull(stats);
            assertFalse(stats.isEmpty());
            Optional<Map<String, Object>> alertStat = stats.stream()
                    .filter(s -> "ALERT".equals(s.get(NAME))).findFirst();
            assertTrue(alertStat.isPresent());
            assertEquals(1, alertStat.get().get(READ));
            assertEquals(1, alertStat.get().get(UNREAD));
            Optional<Map<String, Object>> updateStat = stats.stream()
                    .filter(s -> "UPDATE".equals(s.get(NAME))).findFirst();
            assertTrue(updateStat.isPresent());
            assertEquals(0, updateStat.get().get(READ));
            assertEquals(1, updateStat.get().get(UNREAD));
        }

        @Test
        @DisplayName("should strip internal fields from response notifications")
        void responseNotifications_shouldNotContainInternalFields() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            Map<String, Object> notificationRecord = buildNotification("n1", now, "ALERT", false, false);
            notificationRecord.put(Constants.UPDATED_AT, now);
            notificationRecord.put(Constants.USER_ID, USER_ID_VAL);
            notificationRecord.put(Constants.READ_AT, now);
            notificationRecord.put(Constants.TEMPLATE_ID, "tmpl-1");
            mockCassandraReturn(new ArrayList<>(List.of(notificationRecord)));
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            Map<String, Object> result = response.getResult();
            List<Map<String, Object>> notifications = (List<Map<String, Object>>) result.get(NOTIFICATIONS);
            assertEquals(1, notifications.size());
            Map<String, Object> notif = notifications.get(0);
            assertFalse(notif.containsKey(Constants.IS_DELETED));
            assertFalse(notif.containsKey(Constants.UPDATED_AT));
            assertFalse(notif.containsKey(Constants.USER_ID));
            assertFalse(notif.containsKey(Constants.READ_AT));
            assertFalse(notif.containsKey(Constants.TEMPLATE_ID));
        }

        @Test
        @DisplayName("should parse message JSON string into JsonNode in response")
        void messageField_isParsedToJsonNode() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now, "ALERT", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            Map<String, Object> result = response.getResult();
            List<Map<String, Object>> notifications = (List<Map<String, Object>>) result.get(NOTIFICATIONS);
            Object message = notifications.get(0).get(Constants.MESSAGE);
            assertInstanceOf(JsonNode.class, message);
        }

        @Test
        @DisplayName("should format created_at as ISO string in response")
        void createdAt_isFormattedAsIsoString() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now, "ALERT", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            Map<String, Object> result = response.getResult();
            List<Map<String, Object>> notifications = (List<Map<String, Object>>) result.get(NOTIFICATIONS);
            Object createdAt = notifications.get(0).get(CREATED_AT);
            assertInstanceOf(String.class, createdAt);
            assertDoesNotThrow(() -> Instant.parse((String) createdAt));
        }

        @Test
        @DisplayName("should return INTERNAL_SERVER_ERROR when unexpected exception occurs")
        void unexpectedException_returnsInternalError() {
            mockValidUser();
            when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    anyString(), anyString(), anyMap(), anyList(), anyInt()
            )).thenThrow(new RuntimeException("DB connection failed"));
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
            assertEquals(Constants.FAILED, response.getParams().getStatus());
            assertEquals(ERR_FETCHING_NOTIFICATION_LIST, response.getParams().getErrMsg());
        }

        @Test
        @DisplayName("should handle notifications with null created_at gracefully")
        void nullCreatedAt_isHandledGracefully() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            Map<String, Object> recordWithNull = buildNotification("n1", null, "ALERT", false, false);
            recordWithNull.put(CREATED_AT, null);
            Map<String, Object> validRecord = buildNotification("n2", now, "UPDATE", false, false);
            mockCassandraReturn(new ArrayList<>(List.of(recordWithNull, validRecord)));
            ApiResponse response = service.getMandatoryNotificationsList(
                    AUTH_TOKEN, 30, 0, 10, NotificationReadStatus.BOTH, null);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            assertEquals(1, result.get(TOTAL_COUNT));
        }
    }

    @Nested
    @DisplayName("getCurrentMandatoryNotification")
    class GetCurrentMandatoryNotificationTests {
        @Test
        @DisplayName("should return BAD_REQUEST when auth token is invalid")
        void invalidToken_returnsBadRequest() {
            mockInvalidUser();
            ApiResponse response = service.getCurrentMandatoryNotification(AUTH_TOKEN);
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals(Constants.FAILED, response.getParams().getStatus());
        }

        @Test
        @DisplayName("should return empty map when no unread notifications exist")
        void noUnreadNotifications_returnsEmptyMap() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now.minus(1, ChronoUnit.HOURS), "ALERT", true, false),
                    buildNotification("n2", now.minus(2, ChronoUnit.HOURS), "UPDATE", true, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getCurrentMandatoryNotification(AUTH_TOKEN);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            Map<String, Object> notification = (Map<String, Object>) result.get(NOTIFICATION);
            assertTrue(notification.isEmpty());
        }

        @Test
        @DisplayName("should return empty map when all notifications are deleted")
        void allDeleted_returnsEmptyMap() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1", now, "ALERT", false, true)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getCurrentMandatoryNotification(AUTH_TOKEN);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            Map<String, Object> notification = (Map<String, Object>) result.get(NOTIFICATION);
            assertTrue(notification.isEmpty());
        }

        @Test
        @DisplayName("should return the oldest unread notification")
        void multipleUnread_returnsOldest() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("newest", now.minus(1, ChronoUnit.HOURS), "ALERT", false, false),
                    buildNotification("oldest", now.minus(5, ChronoUnit.HOURS), "UPDATE", false, false),
                    buildNotification("middle", now.minus(3, ChronoUnit.HOURS), "ALERT", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getCurrentMandatoryNotification(AUTH_TOKEN);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            Map<String, Object> notification = (Map<String, Object>) result.get(NOTIFICATION);
            assertEquals("oldest", notification.get(NOTIFICATION_ID));
        }

        @Test
        @DisplayName("should skip read notifications and return oldest unread")
        void mixedReadUnread_returnsOldestUnread() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            List<Map<String, Object>> records = new ArrayList<>(List.of(
                    buildNotification("n1-read", now.minus(5, ChronoUnit.HOURS), "ALERT", true, false),
                    buildNotification("n2-unread-newer", now.minus(1, ChronoUnit.HOURS), "UPDATE", false, false),
                    buildNotification("n3-unread-older", now.minus(3, ChronoUnit.HOURS), "ALERT", false, false)
            ));
            mockCassandraReturn(records);
            ApiResponse response = service.getCurrentMandatoryNotification(AUTH_TOKEN);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            Map<String, Object> notification = (Map<String, Object>) result.get(NOTIFICATION);
            assertEquals("n3-unread-older", notification.get(NOTIFICATION_ID));
        }

        @Test
        @DisplayName("should return empty map when no notifications at all")
        void emptyDatabase_returnsEmptyMap() {
            mockValidUser();
            mockCassandraReturn(new ArrayList<>());
            ApiResponse response = service.getCurrentMandatoryNotification(AUTH_TOKEN);
            assertEquals(HttpStatus.OK, response.getResponseCode());
            Map<String, Object> result = response.getResult();
            Map<String, Object> notification = (Map<String, Object>) result.get(NOTIFICATION);
            assertTrue(notification.isEmpty());
        }

        @Test
        @DisplayName("should strip internal fields from the returned notification")
        void responseNotification_shouldNotContainInternalFields() throws Exception {
            mockValidUser();
            mockObjectMapperReadTree();
            Instant now = Instant.now();
            Map<String, Object> notificationRecord = buildNotification("n1", now, "ALERT", false, false);
            notificationRecord.put(Constants.UPDATED_AT, now);
            notificationRecord.put(Constants.USER_ID, USER_ID_VAL);
            notificationRecord.put(Constants.READ_AT, now);
            notificationRecord.put(Constants.TEMPLATE_ID, "tmpl-1");
            mockCassandraReturn(new ArrayList<>(List.of(notificationRecord)));
            ApiResponse response = service.getCurrentMandatoryNotification(AUTH_TOKEN);
            Map<String, Object> result = response.getResult();
            Map<String, Object> notification = (Map<String, Object>) result.get(NOTIFICATION);
            assertFalse(notification.containsKey(Constants.IS_DELETED));
            assertFalse(notification.containsKey(Constants.UPDATED_AT));
            assertFalse(notification.containsKey(Constants.USER_ID));
            assertFalse(notification.containsKey(Constants.READ_AT));
            assertFalse(notification.containsKey(Constants.TEMPLATE_ID));
        }

        @Test
        @DisplayName("should return INTERNAL_SERVER_ERROR on unexpected exception")
        void unexpectedException_returnsInternalError() {
            mockValidUser();
            when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    anyString(), anyString(), anyMap(), anyList(), anyInt()
            )).thenThrow(new RuntimeException("DB error"));
            ApiResponse response = service.getCurrentMandatoryNotification(AUTH_TOKEN);
            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
            assertEquals(Constants.FAILED, response.getParams().getStatus());
            assertEquals(ERR_FETCHING_NOTIFICATION, response.getParams().getErrMsg());
        }
    }


    @Nested
    @DisplayName("markMandatoryNotificationsAsRead")
    class MarkMandatoryNotificationsAsReadTests {
        private Map<String, Object> buildRequestBody(String id, String createdAt) {
            Map<String, Object> request = new HashMap<>();
            if (id != null) request.put(ID, id);
            if (createdAt != null) request.put(CREATED_AT, createdAt);
            return Map.of(Constants.REQUEST, request);
        }

        @Test
        @DisplayName("should return BAD_REQUEST when auth token is invalid")
        void invalidToken_returnsBadRequest() {
            mockInvalidUser();
            ApiResponse response = service.markMandatoryNotificationsAsRead(
                    AUTH_TOKEN, buildRequestBody("n1", "2026-02-28T08:00:00Z"));
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals(Constants.FAILED, response.getParams().getStatus());
        }

        static Stream<Arguments> invalidMarkAsReadInputs() {
            return Stream.of(
                    Arguments.of(null, "2026-02-28T08:00:00Z", ERR_ID_AND_CREATED_AT_REQUIRED),
                    Arguments.of("n1", null, ERR_ID_AND_CREATED_AT_REQUIRED),
                    Arguments.of(null, null, ERR_ID_AND_CREATED_AT_REQUIRED),
                    Arguments.of("", "2026-02-28T08:00:00Z", ERR_ID_AND_CREATED_AT_REQUIRED),
                    Arguments.of("n1", "not-a-date", ERR_INVALID_CREATED_AT_FORMAT)
            );
        }

        @ParameterizedTest(name = "should return BAD_REQUEST for id={0}, createdAt={1}")
        @MethodSource("invalidMarkAsReadInputs")
        void invalidInput_returnsBadRequest(String id, String createdAt, String expectedErrMsg) {
            mockValidUser();
            ApiResponse response = service.markMandatoryNotificationsAsRead(
                    AUTH_TOKEN, buildRequestBody(id, createdAt));
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals(expectedErrMsg, response.getParams().getErrMsg());
        }

        @Test
        @DisplayName("should mark notification as read successfully")
        void validRequest_marksAsRead() {
            mockValidUser();
            String createdAtStr = "2026-02-28T08:00:00Z";
            when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_MANDATORY_NOTIFICATION),
                    anyMap(), anyList(), eq(1)
            )).thenReturn(List.of(Map.of(NOTIFICATION_ID, "n1")));
            when(cassandraOperation.updateRecordByCompositeKey(
                    eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_MANDATORY_NOTIFICATION),
                    anyMap(), anyMap()
            )).thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
            ApiResponse response = service.markMandatoryNotificationsAsRead(
                    AUTH_TOKEN, buildRequestBody("n1", createdAtStr));
            assertEquals(HttpStatus.OK, response.getResponseCode());
            assertEquals(Constants.SUCCESS, response.getParams().getStatus());
            assertEquals(MSG_NOTIFICATION_MARKED_READ, response.getParams().getErrMsg());
            Map<String, Object> result = response.getResult();
            assertEquals("n1", result.get(ID));
            assertEquals(true, result.get(READ));
            assertNotNull(result.get(READ_AT));
        }

        @Test
        @DisplayName("should return NOT_FOUND when notification does not exist")
        void notificationNotFound_returnsNotFound() {
            mockValidUser();
            when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_MANDATORY_NOTIFICATION),
                    anyMap(), anyList(), eq(1)
            )).thenReturn(Collections.emptyList());
            ApiResponse response = service.markMandatoryNotificationsAsRead(
                    AUTH_TOKEN, buildRequestBody("n1", "2026-02-28T08:00:00Z"));
            assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
            assertEquals(ERR_NOTIFICATION_NOT_FOUND, response.getParams().getErrMsg());
            verify(cassandraOperation, never()).updateRecordByCompositeKey(anyString(), anyString(), anyMap(), anyMap());
        }

        @Test
        @DisplayName("should pass correct composite key to Cassandra")
        void validRequest_passesCorrectCompositeKey() {
            mockValidUser();
            String createdAtStr = "2026-02-28T08:00:00Z";
            when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_MANDATORY_NOTIFICATION),
                    anyMap(), anyList(), eq(1)
            )).thenReturn(List.of(Map.of(NOTIFICATION_ID, "n1")));
            when(cassandraOperation.updateRecordByCompositeKey(
                    anyString(), anyString(), anyMap(), anyMap()
            )).thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
            service.markMandatoryNotificationsAsRead(
                    AUTH_TOKEN, buildRequestBody("n1", createdAtStr));
            ArgumentCaptor<Map<String, Object>> compositeKeyCaptor = ArgumentCaptor.forClass(Map.class);
            ArgumentCaptor<Map<String, Object>> updateCaptor = ArgumentCaptor.forClass(Map.class);
            verify(cassandraOperation).updateRecordByCompositeKey(
                    eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_MANDATORY_NOTIFICATION),
                    updateCaptor.capture(), compositeKeyCaptor.capture()
            );
            Map<String, Object> compositeKey = compositeKeyCaptor.getValue();
            assertEquals(USER_ID_VAL, compositeKey.get(Constants.USER_ID));
            assertEquals(Instant.parse(createdAtStr), compositeKey.get(Constants.CREATED_AT));
            Map<String, Object> updateAttrs = updateCaptor.getValue();
            assertEquals(true, updateAttrs.get(Constants.READ));
            assertNotNull(updateAttrs.get(Constants.READ_AT));
        }

        @Test
        @DisplayName("should return INTERNAL_SERVER_ERROR when Cassandra update fails")
        void cassandraUpdateFails_returnsInternalError() {
            mockValidUser();
            when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_MANDATORY_NOTIFICATION),
                    anyMap(), anyList(), eq(1)
            )).thenReturn(List.of(Map.of(NOTIFICATION_ID, "n1")));
            when(cassandraOperation.updateRecordByCompositeKey(
                    anyString(), anyString(), anyMap(), anyMap()
            )).thenReturn(Map.of(Constants.RESPONSE, "failure"));
            ApiResponse response = service.markMandatoryNotificationsAsRead(
                    AUTH_TOKEN, buildRequestBody("n1", "2026-02-28T08:00:00Z"));
            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
            assertEquals(ERR_FAILED_TO_UPDATE_NOTIFICATION, response.getParams().getErrMsg());
        }

        @Test
        @DisplayName("should return INTERNAL_SERVER_ERROR on unexpected exception")
        void unexpectedException_returnsInternalError() {
            mockValidUser();
            when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_MANDATORY_NOTIFICATION),
                    anyMap(), anyList(), eq(1)
            )).thenReturn(List.of(Map.of(NOTIFICATION_ID, "n1")));
            when(cassandraOperation.updateRecordByCompositeKey(
                    anyString(), anyString(), anyMap(), anyMap()
            )).thenThrow(new RuntimeException("Connection timeout"));
            ApiResponse response = service.markMandatoryNotificationsAsRead(
                    AUTH_TOKEN, buildRequestBody("n1", "2026-02-28T08:00:00Z"));
            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
            assertEquals(Constants.FAILED, response.getParams().getStatus());
            assertEquals(ERR_UPDATING_NOTIFICATION, response.getParams().getErrMsg());
        }
    }
}
