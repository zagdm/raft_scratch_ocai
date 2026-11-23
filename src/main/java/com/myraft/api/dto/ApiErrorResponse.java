package com.myraft.api.dto;

/** Однообразный ответ об ошибке клиентского API. */
public class ApiErrorResponse {
    private String error;
    private String leaderId;

    public ApiErrorResponse() {
    }

    public ApiErrorResponse(String error, String leaderId) {
        this.error = error;
        this.leaderId = leaderId;
    }

    public static ApiErrorResponse of(String error) {
        return new ApiErrorResponse(error, null);
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }
}
