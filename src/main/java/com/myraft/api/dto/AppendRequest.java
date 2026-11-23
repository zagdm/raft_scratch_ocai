package com.myraft.api.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** Тело {@code POST /raft/log}: пользовательская команда KV. */
public class AppendRequest {
    private String key;
    private JsonNode value;

    public AppendRequest() {
    }

    public AppendRequest(String key, JsonNode value) {
        this.key = key;
        this.value = value;
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
