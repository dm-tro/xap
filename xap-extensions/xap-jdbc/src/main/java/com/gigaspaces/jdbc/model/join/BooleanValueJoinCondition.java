package com.gigaspaces.jdbc.model.join;

public class BooleanValueJoinCondition implements JoinCondition {
    private final boolean value;

    public BooleanValueJoinCondition(boolean value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public boolean isOperator() {
        return false;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
