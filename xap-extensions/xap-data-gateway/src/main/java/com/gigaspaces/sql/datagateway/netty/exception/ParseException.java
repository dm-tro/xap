package com.gigaspaces.sql.datagateway.netty.exception;

import com.gigaspaces.sql.datagateway.netty.utils.ErrorCodes;

/**
 * Generic ParseException
 */
public class ParseException extends NonBreakingException {
    public ParseException(String message, Throwable cause) {
        super(ErrorCodes.SYNTAX_ERROR, message, cause);
    }
}
