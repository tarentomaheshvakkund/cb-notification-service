package com.igot.cb.util;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
public class CbServerProperties {


  @Value("${redis.insights.index}")
  private int redisInsightIndex;

  @Value("${search.result.redis.ttl}")
  private long searchResultRedisTtl;

  @Value("${sb.api.key}")
  private String sbApiKey;

  @Value("${http.client.request.factory.timeout}")
  private int requestTimeoutMs;

  @Value("${http.pooling.client.cm.max.total.connections}")
  private int maxTotalConnections;

  @Value("${http.pooling.client.cm.default.max.per.route}")
  private int maxConnectionsPerRoute;

  @Value("${redis.pool.max.total}")
  private int redisPoolMaxTotal;

  @Value("${redis.pool.max.idle}")
  private int redisPoolMaxIdle;

  @Value("${redis.pool.min.idle}")
  private int redisPoolMinIdle;

  @Value("${redis.pool.max.wait}")
  private int redisPoolMaxWait;

  @Value("${redis.connection.timeout}")
  private long redisConnectionTimeout;

  @Value("${spring.kafka.bootstrap.servers}")
  private String springKafkaBootStrapServers;

  @Value("${peervalidation.bulk.user.notification.limit}")
  private int peerValidationBulkUserNotificationLimit;

  @Value("${peervalidation.notification.setting.check.enabled}")
  private boolean peerValidationNotificationSettingCheckEnabled;

  @Value("${peervalidation.list.max.fetch}")
  private int peerValidationListMaxFetch;

  @Value("${kafka.topic.process.peer.validation.error}")
  private String kafkaTopicPeerValidationError;

  @Value("${kafka.topic.notification.read.event}")
  private String kafkaTopicNotificationReadEvent;

  @Value("${peervalidation.bulk.created.at.offset.ms}")
  private long peerValidationBulkCreatedAtOffsetMs;

  @Value("${kafka.topic.process.peer.evaluation.error}")
  private String kafkaTopicPeerEvaluationError;

  @Value("${kafka.topic.notification.bulk.create.error}")
  private String kafkaTopicNotificationBulkCreateError;

  @Value("#{'${notification.list.peer.evaluation.assigned.excluded.statuses}'.split(',')}")
  private List<String> peerEvaluationAssignedExcludedStatuses;

  @Value("#{'${notification.list.peer.review.assigned.excluded.statuses}'.split(',')}")
  private List<String> peerReviewAssignedExcludedStatuses;

  @Value("${cleanup.peer.validation.consumer.group.id}")
  private String cleanupConsumerGroupId;

  @Value("${cleanup.peer.validation.kafka.topics}")
  private String cleanupKafkaTopics;

  @Value("${cleanup.peer.validation.batch.size}")
  private int cleanupBatchSize;

  @Value("${cleanup.peer.validation.thread.pool.size}")
  private int cleanupThreadPoolSize;

  @Value("${cleanup.peer.validation.table.requests}")
  private String cleanupTableRequests;

  @Value("${cleanup.peer.validation.table.reviews}")
  private String cleanupTableReviews;

  @Value("${cleanup.peer.validation.table.audit}")
  private String cleanupTableAudit;

  @Value("${cleanup.peer.validation.window.start.time}")
  private String cleanupWindowStartTime;

  @Value("${cleanup.peer.validation.window.end.time}")
  private String cleanupWindowEndTime;

  @Value("${cleanup.peer.validation.day.offset}")
  private long cleanupDayOffset;

  @Value("${cleanup.peer.validation.poll.timeout.seconds}")
  private int cleanupPollTimeoutSeconds;

  @Value("${cleanup.peer.validation.executor.shutdown.timeout.minutes}")
  private int cleanupExecutorShutdownTimeoutMinutes;

  @Value("${cleanup.peer.validation.audit.deletion.prefix}")
  private String cleanupAuditDeletionPrefix;
}
