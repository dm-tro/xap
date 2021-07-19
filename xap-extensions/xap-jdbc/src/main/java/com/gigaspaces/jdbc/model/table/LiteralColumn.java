package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;

public class LiteralColumn implements IQueryColumn{
    private final Object value;
    private int columnOrdinal;

    public LiteralColumn(Object value, int columnOrdinal) {
        this.value = value;
        this.columnOrdinal = columnOrdinal;
    }

    @Override
    public int getColumnOrdinal() {
        return columnOrdinal;
    }

    @Override
    public void setColumnOrdinal(int ordinal) {
        throw new UnsupportedOperationException("Unsupported method setColumnOrdinal");
    }

    @Override
    public String getName() {
        return "\'" + value + "\'";
    }

    @Override
    public String getAlias() {
        return getName();
    }

    @Override
    public boolean isVisible() {
        throw new UnsupportedOperationException("Unsupported method");
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
        return Integer.valueOf(columnOrdinal).compareTo(o.getColumnOrdinal());
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        return value;
    }
}
