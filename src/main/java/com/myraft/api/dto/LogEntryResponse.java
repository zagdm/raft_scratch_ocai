package com.myraft.api.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** Пользовательская запись лога; NoOpCommand в этот список не попадает. */
public class LogEntryResponse {
    private long index;
    private int term;
    private String key;
    private JsonNode value;

    public LogEntryResponse() {
    }

    public LogEntryResponse(long index, int term, String key, JsonNode value) {
        this.index = index;
        this.term = term;
        this.key = key;
        this.value = value;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public JsonNode getValue() {
        return value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }
}
