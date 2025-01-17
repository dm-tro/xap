package com.gigaspaces.sql.datagateway.netty.exception;

public class BreakingException extends ProtocolException {
    public BreakingException(String code, String message) {
        super(code, message);
    }

    public BreakingException(String code, String message, Throwable cause) {
        super(code, message, cause);
    }

    @Override
    public boolean closeSession() {
        return false;
    }
}
