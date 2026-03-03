package com.igot.cb.notification.service;

import com.igot.cb.notification.enums.NotificationReadStatus;
import org.igot.common.ApiResponse;

import java.util.Map;

public interface MandatoryNotificationService {

    ApiResponse getMandatoryNotificationsList(String token, int days, int page, int size, NotificationReadStatus status, String subType);

    ApiResponse getCurrentMandatoryNotification(String token);

    ApiResponse markMandatoryNotificationsAsRead(String token, Map<String, Object> requestBody);
}
