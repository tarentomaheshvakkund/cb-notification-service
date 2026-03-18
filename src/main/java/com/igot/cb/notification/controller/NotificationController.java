package com.igot.cb.notification.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.peervalidationcleanup.service.PeerValidationCleanupService;
import com.igot.cb.notification.enums.NotificationReadStatus;
import com.igot.cb.notification.service.NotificationService;
import com.igot.cb.util.Constants;

import org.igot.common.ApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.igot.cb.util.Constants.*;

@RestController
@RequestMapping("/v1/notifications")
public class NotificationController {

    private NotificationService notificationService;
    private final PeerValidationCleanupService peerValidationCleanupService;

    public NotificationController(NotificationService notificationService,
                                  PeerValidationCleanupService peerValidationCleanupService) {
        this.notificationService = notificationService;
        this.peerValidationCleanupService = peerValidationCleanupService;
    }

    @PostMapping("/create")
    public ResponseEntity<ApiResponse> createNotification(@RequestBody JsonNode userNotificationDetail,
                                                          @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = notificationService.createNotification(userNotificationDetail, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/bulk/create")
    public ResponseEntity<ApiResponse> createBulkNotification(
            @RequestBody JsonNode userNotificationDetail) {
        ApiResponse response = notificationService.bulkCreateNotifications(userNotificationDetail);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/readby/{notificationId}")
    public ResponseEntity<ApiResponse> readByUserIdAndNotificationId(@PathVariable String notificationId, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = notificationService.readByUserIdAndNotificationId(notificationId, token);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }


    @GetMapping("/list")
    public ResponseEntity<ApiResponse> getLastXDaysNotifications(
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestParam(defaultValue = Constants.DEFAULT_NOTIFICATION_DAYS + "") int days,
            @RequestParam(defaultValue = Constants.DEFAULT_NOTIFICATION_PAGE + "") int page,
            @RequestParam(defaultValue = Constants.DEFAULT_NOTIFICATION_PAGE_SIZE + "") int size,
            @RequestParam(defaultValue = Constants.DEFAULT_NOTIFICATION_READ_STATUS) NotificationReadStatus status,
            @RequestParam(required = false) String subType) {

        ApiResponse response = notificationService.getNotificationsByUserIdAndLastXDays(token, days, page, size, status, subType);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }


    @PatchMapping("/read")
    public ResponseEntity<ApiResponse> markNotificationsAsRead(
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestBody Map<String, Object> requestBody) {

        Map<String, Object> request = (Map<String, Object>) requestBody.get(REQUEST);
        ApiResponse response = notificationService.markNotificationsAsRead(token, request, Constants.API_VERSION_V1);
        return ResponseEntity.ok(response);
    }


    @DeleteMapping("/delete")
    public ResponseEntity<ApiResponse> markNotificationsAsDeleted(
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestBody Map<String, Object> requestBody) {

        List<String> ids = (List<String>) ((Map<String, Object>) requestBody.get(REQUEST)).get(IDS);
        ApiResponse response = notificationService.markNotificationsAsDeleted(token, ids);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/unread/count")
    public ResponseEntity<ApiResponse> getUnreadNotificationCount(
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestParam(defaultValue = Constants.DEFAULT_NOTIFICATION_DAYS + "") int days) {

        ApiResponse response = notificationService.getUnreadNotificationCount(token, days);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/reset/unread/count")
    public ResponseEntity<ApiResponse> getResetNotificationCount(
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {

        ApiResponse response = notificationService.getResetNotificationCount(token);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }


    @PostMapping(Constants.BULK_CREATE_PEER_VALIDATION_ENDPOINT)
    public ResponseEntity<Map<String, Object>> createBulkPeerValidationNotification(
            @RequestBody Map<String, Object> requestBody) {
        ApiResponse apiResponse = notificationService.bulkCreatePeerValidationNotifications(requestBody);
        Map<String, Object> responseBody = new LinkedHashMap<>();
        responseBody.put(ID, apiResponse.getId());
        responseBody.put(VER, apiResponse.getVer());
        responseBody.put(TS, apiResponse.getTs());
        responseBody.put(PARAMS, apiResponse.getParams());
        responseBody.put(RESPONSE_CODE, apiResponse.getResponseCode());
        responseBody.put(RESULT, apiResponse.getResult());
        return new ResponseEntity<>(responseBody, apiResponse.getResponseCode());
    }

    @GetMapping(Constants.PEER_VALIDATION_LIST_ENDPOINT)
    public ResponseEntity<Map<String, Object>> getPeerValidationNotifications(
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestParam String subType,
            @RequestParam(defaultValue = Constants.DEFAULT_NOTIFICATION_DAYS + "") int days,
            @RequestParam(defaultValue = Constants.DEFAULT_NOTIFICATION_PAGE + "") int page,
            @RequestParam(defaultValue = Constants.DEFAULT_NOTIFICATION_PAGE_SIZE + "") int size) {

        ApiResponse apiResponse = notificationService.getPeerValidationNotifications(token, subType, days, page, size);
        Map<String, Object> responseBody = new LinkedHashMap<>();
        responseBody.put(ID, apiResponse.getId());
        responseBody.put(VER, apiResponse.getVer());
        responseBody.put(TS, apiResponse.getTs());
        responseBody.put(PARAMS, apiResponse.getParams());
        responseBody.put(RESPONSE_CODE, apiResponse.getResponseCode());
        responseBody.put(RESULT, apiResponse.getResult());
        return new ResponseEntity<>(responseBody, apiResponse.getResponseCode());
    }

    @PatchMapping(Constants.MARK_NOTIFICATIONS_READ_V2_ENDPOINT)
    public ResponseEntity<ApiResponse> markNotificationsAsReadV2(
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestBody Map<String, Object> requestBody) {
        Map<String, Object> request = (Map<String, Object>) requestBody.get(REQUEST);
        ApiResponse response = notificationService.markNotificationsAsRead(token, request, Constants.API_VERSION_V2);
        return ResponseEntity.ok(response);
    }


    @PostMapping(Constants.CLEANUP_PEER_VALIDATION_ENDPOINT)
    public ResponseEntity<Void> runPeerValidationCleanup() {
        peerValidationCleanupService.runCleanup(Instant.now());
        return ResponseEntity.accepted().build();
    }

}
