package com.igot.cb.util;

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

  @Value("${mandatory.notification.max.fetch.limit:100}")
  private int mandatoryNotificationMaxFetchLimit;

}
