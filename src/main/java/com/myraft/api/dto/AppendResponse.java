package com.myraft.api.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** Ответ вставки: результат apply и индекс записи в логе. */
public class AppendResponse {
    private JsonNode previousValue;
    private JsonNode value;
    private long index;

    public AppendResponse() {
    }

    public AppendResponse(JsonNode previousValue, JsonNode value, long index) {
        this.previousValue = previousValue;
        this.value = value;
        this.index = index;
    }

    public JsonNode getPreviousValue() {
        return previousValue;
    }

    public void setPreviousValue(JsonNode previousValue) {
        this.previousValue = previousValue;
    }

    public JsonNode getValue() {
        return value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }
}
