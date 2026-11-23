package com.myraft.raft;

/**
 * Запись не была применена к state machine за отведённое время ожидания.
 * API-адаптер превращает это в отдельный HTTP-статус.
 */
public class ApplyTimeoutException extends RuntimeException {

    public ApplyTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
