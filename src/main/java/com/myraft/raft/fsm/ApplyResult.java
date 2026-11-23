package com.myraft.raft.fsm;

/**
 * Результат применения одной команды к state machine.
 * <p>
 * Ядро Raft хранит результат как непрозрачный {@code ApplyResult<?>}; конкретный
 * API-адаптер знает тип {@code R} и превращает значение в ответ клиенту.
 */
public record ApplyResult<R>(R previousValue, R value, long index) {
}
