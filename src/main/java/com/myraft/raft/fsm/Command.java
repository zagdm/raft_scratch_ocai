package com.myraft.raft.fsm;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Маркер пользовательской команды state machine.
 * <p>
 * Интерфейс открыт для расширения: конкретные команды регистрируются во внешнем
 * Jackson subtype registry (см. {@code KvCommandsModule}), а не в ядре Raft.
 * Полиморфизм для сериализации задаётся свойством {@code type}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface Command {
}
