package com.custom.recommend_kafka.producer;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 공통 Kafka 전송 프로듀서
 * - 모든 MSA Service는 이 클래스를 주입받아 사용
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Kafka로 이벤트를 전송 처리
     * @param <K> Key의 타입 (Long, String 등)
     * @param <V> Payload의 타입 (DTO, String 등)
     * @param topic 전송할 토픽 이름
     * @param key 메시지 순서 보장을 위한 키 (내부에서 String으로 변환됨)
     * @param payload 전송할 데이터 객체
     */
    public <K, V> void send(String topic, K key, V payload) {
        
        // 1. Key 타입 변환
        String stringKey = (key == null) ? null : String.valueOf(key);

        // 2. 비동기 전송 시작
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, stringKey, payload);

        // 3. 콜백 처리 (성공/실패 로깅)
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // 성공 시: 디버그 혹은 정보 로그
                log.info("Kafka Send Success: topic=[{}], key=[{}], partition=[{}], offset=[{}]",
                        topic,
                        stringKey,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                // 실패 시: 에러 로그 및 예외 스택 트레이스 포함
                log.error("Kafka Send Failed: topic=[{}], key=[{}], payload=[{}]",
                        topic, stringKey, payload, ex);
            }
        });
    }
}