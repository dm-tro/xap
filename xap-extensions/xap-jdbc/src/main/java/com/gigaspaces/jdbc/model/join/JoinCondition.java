package com.gigaspaces.jdbc.model.join;

public interface JoinCondition {

    Object getValue();

    boolean isOperator();
}
