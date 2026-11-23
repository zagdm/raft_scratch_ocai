package com.myraft.raft.fsm.kv;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Реестр KV-команд для Jackson: конкретные типы подключаются извне ядра Raft.
 * Неизвестный тип при десериализации приводит к ошибке (Jackson fail-on-invalid-subtype).
 */
public class KvCommandsModule extends SimpleModule {

    public KvCommandsModule() {
        super("kv-commands");
        registerSubtypes(new NamedType(PutCommand.class, "PUT"));
    }
}
