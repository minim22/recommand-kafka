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
     * @param topic 전송할 토픽 이름
     * @param key 메시지 순서 보장을 위한 키
     * @param payload payload 전송할 데이터 객체
     */
    public void send(String topic, String key, Object payload) {
        
        // 1. 비동기 전송 시작
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, payload);

        // 2. 콜백 처리 (성공/실패 로깅)
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // 성공 시: 디버그 로그 (운영 환경 노이즈 방지)
                // result.getRecordMetadata()를 통해 파티션과 오프셋 확인 가능
                log.debug("Kafka Send Success: topic=[{}], key=[{}], offset=[{}]",
                        topic, key, result.getRecordMetadata().offset());
            } else {
                // 실패 시: 에러 로그 (추후 모니터링 알람 연동 포인트)
                log.error("Kafka Send Failed: topic=[{}], key=[{}], payload=[{}]",
                        topic, key, payload, ex);
            }
        });
    }
}