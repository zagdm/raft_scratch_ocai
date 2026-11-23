package com.myraft.raft.fsm;

/**
 * Абстракция детерминированной replicated state machine.
 * <p>
 * Ядро Raft вызывает {@link #apply(long, Command)} только для пользовательских команд
 * из {@code UserCommand}; Raft-уровневые {@code NoOpCommand} до state machine не доходят.
 * <p>
 * Требования к конкретной реализации:
 * <ul>
 *     <li>детерминизм: одинаковая последовательность команд даёт одинаковый результат;</li>
 *     <li>идемпотентность по индексу: повторный apply уже применённой записи ничего не меняет
 *         (порядок и границы обеспечиваются ядром через {@code lastApplied});</li>
 *     <li>быстрый apply без сетевых/дисковых побочных эффектов — вызывается под {@code nodeLock}.</li>
 * </ul>
 *
 * @param <R> тип значения, возвращаемого state machine
 */
public interface StateMachine<R> {

    ApplyResult<R> apply(long index, Command command);

    /** Заложено на будущее: в этапах 2–4 снапшоты не реализуются. */
    default Snapshot snapshot() {
        throw new UnsupportedOperationException("snapshots are not supported yet");
    }

    /** Заложено на будущее: в этапах 2–4 снапшоты не реализуются. */
    default void restore(Snapshot snapshot) {
        throw new UnsupportedOperationException("snapshots are not supported yet");
    }
}
