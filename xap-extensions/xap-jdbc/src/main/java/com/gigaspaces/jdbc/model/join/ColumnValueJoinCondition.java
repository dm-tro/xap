package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.IQueryColumn;

public class ColumnValueJoinCondition implements JoinCondition {
    private final IQueryColumn column;

    public ColumnValueJoinCondition(IQueryColumn column) {
        this.column = column;
    }

    @Override
    public Object getValue() {
        return column.getCurrentValue();
    }

    @Override
    public boolean isOperator() {
        return false;
    }

    public IQueryColumn getColumn() {
        return this.column;
    }

    @Override
    public String toString() {
        return column + ": " + getValue();
    }
}
