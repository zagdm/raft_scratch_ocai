package com.myraft.config;

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

    /** Конкретный тип нужен API-адаптеру для чтения KV; для ядра это по-прежнему {@link StateMachine}. */
    @Bean
    public KvStateMachine stateMachine() {
        return new KvStateMachine();
    }
}
