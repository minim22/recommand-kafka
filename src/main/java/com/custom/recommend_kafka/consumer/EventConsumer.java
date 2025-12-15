package com.custom.recommend_kafka.consumer;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 공통 Kafka 수신 프로듀서
 * - 모든 MSA Service는 이 클래스를 주입받아 사용
 */
@Slf4j
@Component
@RequiredArgsConstructor
public abstract class EventConsumer<T> {
    
    private final ObjectMapper objectMapper;

    /**
     * [핵심] 자식 클래스가 구현해야 할 비즈니스 로직
     * @param payload 파싱된 데이터 객체
     * @param key 메시지 키 (필요 시 사용)
     */
    protected abstract void onMessageReceived(T payload, String key);

    /**
     * Kafka로 이벤트를 수신 처리 (공통 로직)
     * - 자식 클래스의 @KafkaListener에서 이 메서드를 호출함
     * * @param record 카프카 원본 레코드 (Topic, Key, Value 모두 포함)
     * @param typeRef 변환할 타입 정보 (List 등을 처리하기 위함)
     * @param ack 수동 커밋을 위한 객체 (옵션)
     */
    public void receive(ConsumerRecord<String, String> record, TypeReference<T> typeRef, Acknowledgment ack) {
        String topic = record.topic();
        String key = record.key();
        String message = record.value();

        long startTime = System.currentTimeMillis();
        
        try {
            log.debug("Kafka Receive Start: topic=[{}], key=[{}], length=[{}]", topic, key, message.length());

            // 1. JSON -> Object 변환 (공통 처리)
            T payload = objectMapper.readValue(message, typeRef);

            // 2. 자식 클래스의 비즈니스 로직 실행 (추상 메서드 호출)
            onMessageReceived(payload, key);
            
            // 3. (옵션) 수동 커밋 처리
            if (ack != null) {
                ack.acknowledge();
            }

            long duration = System.currentTimeMillis() - startTime;
            log.debug("Kafka Receive Success: topic=[{}], processed in {}ms", topic, duration);

        } catch (IOException e) {
            log.error("Kafka Parsing Error: topic=[{}], key=[{}], message=[{}]", topic, key, message, e);
            // 파싱 에러는 재시도해도 실패하므로 여기서 처리하거나 DLQ로 보냄
        } catch (Exception e) {
            log.error("Kafka Logic Error: topic=[{}], key=[{}]", topic, key, e);
            throw e; // Kafka가 에러를 감지하고 재시도(Retry)할 수 있도록 던짐
        }
    }
}