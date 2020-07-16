package com.pinterest.singer.writer.memq.producer;

public class MemqWriteTaskResult {

    public final boolean success;
    public final Exception exception;

    public MemqWriteTaskResult() {
        this.success = false;
        this.exception = null;
    }

    public MemqWriteTaskResult(boolean success, Exception exception) {
        this.success = success;
        this.exception = exception;
    }

    public boolean isSuccess() {
        return success;
    }

    public Exception getException() {
        return exception;
    }
}
