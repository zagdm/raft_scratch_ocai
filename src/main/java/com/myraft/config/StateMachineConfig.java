package com.myraft.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.myraft.raft.fsm.StateMachine;
import com.myraft.raft.fsm.kv.KvCommandsModule;
import com.myraft.raft.fsm.kv.KvStateMachine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Подключение первой конкретизации state machine и реестра её команд.
 * Ядро Raft зависит только от {@link StateMachine}, не от KV.
 */
@Configuration
public class StateMachineConfig {

    @Bean
    public KvCommandsModule kvCommandsModule() {
        return new KvCommandsModule();
    }

    @Bean
    public StateMachine<JsonNode> stateMachine() {
        return new KvStateMachine();
    }
}
