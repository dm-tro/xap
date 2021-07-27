package com.gigaspaces.sql.datagateway.netty.query;

@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
    T apply() throws E;
}
