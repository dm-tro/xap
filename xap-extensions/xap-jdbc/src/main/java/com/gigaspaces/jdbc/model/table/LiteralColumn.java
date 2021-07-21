package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;

public class LiteralColumn implements IQueryColumn{
    private final Object value;
    private int columnOrdinal;
    private final String alias;

    public LiteralColumn(Object value, int columnOrdinal, String alias) {
        this.value = value;
        this.columnOrdinal = columnOrdinal;
        this.alias = alias;
    }

    @Override
    public int getColumnOrdinal() {
        return columnOrdinal;
    }

    @Override
    public void setColumnOrdinal(int ordinal) {
        columnOrdinal = ordinal;
    }

    @Override
    public String getName() {
        return (alias == null) ? "\'" + value + "\'" : alias;
    }

    @Override
    public String getAlias() {
        return getName();
    }

    @Override
    public boolean isVisible() {
        return columnOrdinal != -1;
    }

    @Override
    public boolean isUUID() {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public TableContainer getTableContainer() {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public Object getCurrentValue() {
        return value;
    }

    @Override
    public Class<?> getReturnType() {
        return value.getClass();
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public int compareTo(IQueryColumn o) {
        return Integer.compare(columnOrdinal, o.getColumnOrdinal());
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        return value;
    }

    @Override
    public String toString() {
        return getAlias();
    }
}
