package com.myraft;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Проверяет, что Spring-контекст собирается: единственный конструктор бинов,
 * зависимости {@code RaftNode}/{@code RaftRpcService} и запуск узла на старте.
 */
@SpringBootTest
class MyRaftApplicationTests {

    @Test
    void contextLoads() {
    }
}
