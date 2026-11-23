package com.myraft.api.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** Результат {@code GET /raft/kv}; found различает отсутствие ключа и JSON null. */
public class KvReadResponse {
    private String key;
    private JsonNode value;
    private boolean found;

    public KvReadResponse() {
    }

    public KvReadResponse(String key, JsonNode value, boolean found) {
        this.key = key;
        this.value = value;
        this.found = found;
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

    public boolean isFound() {
        return found;
    }

    public void setFound(boolean found) {
        this.found = found;
    }
}
