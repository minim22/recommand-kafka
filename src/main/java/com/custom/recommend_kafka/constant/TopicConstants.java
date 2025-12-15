package com.custom.recommend_kafka.constant;


public final class TopicConstants {
    // 1. 영화 데이터 수집 토픽 (Content Service -> Recommend Service)
    public static final String MOVIE_INGESTION_TOPIC = "content.movie.ingestion";

    // 2. 유저 가입 이벤트 토픽 (User Service -> Others)
    public static final String USER_SIGNUP_TOPIC = "user.lifecycle.signup";

    // 3. 유저 활동 로그 토픽 (Activity Service -> Recommend Service)
    public static final String USER_ACTIVITY_TOPIC = "user.activity.log";

    // 인스턴스화 방지 (Utility Class)
    private TopicConstants() {
        throw new IllegalStateException("Utility class");
    }
}