package com.gigaspaces.jdbc.model.table;

import java.util.Locale;
import java.util.Objects;

public class AggregationColumn implements IQueryColumn {

    private final AggregationFunctionType type;
    private final String functionAlias;
    private final boolean isVisible;
    private final boolean allColumns;
    private final IQueryColumn queryColumn;
    private final int columnOrdinal;

    public AggregationColumn(AggregationFunctionType functionType, String functionAlias, IQueryColumn queryColumn,
                             boolean isVisible, boolean allColumns, int columnOrdinal) {
        this.queryColumn = queryColumn;
        this.type = functionType;
        this.functionAlias = setFunctionAlias(functionType, functionAlias, queryColumn, allColumns);
        this.allColumns = allColumns;
        this.isVisible = isVisible;
        this.columnOrdinal = columnOrdinal;
    }

    private String setFunctionAlias(AggregationFunctionType functionType, String functionAlias,
                                    IQueryColumn queryColumn, boolean allColumns) {
        if (functionAlias == null) {
            String columnAlias;
            if (queryColumn == null) {
                columnAlias = allColumns ? "*" : null;
            } else {
                columnAlias = queryColumn.getAlias();
            }
            String functionName = functionType.name().toLowerCase(Locale.ROOT);
            return String.format("%s(%s)", functionName, columnAlias);
        }
        return functionAlias;
    }

    public AggregationFunctionType getType() {
        return this.type;
    }

    private String getFunctionName() {
        return this.type.name().toLowerCase(Locale.ROOT);
    }

    public String getAlias() {
        return this.functionAlias;
    }

    public TableContainer getTableContainer() {
        return this.queryColumn.getTableContainer();
    }

    @Override
    public Object getCurrentValue() {
        return null;
    }

    @Override
    public Class<?> getReturnType() {
        return null;
    }

    public String getColumnName() {
        if (this.queryColumn == null) {
            return isAllColumns() ? "*" : null;
        }
        return this.queryColumn.getAlias();  //TODO: @sagiv use getName instead?
    }

    public boolean isVisible() {
        return this.isVisible;
    }

    @Override
    public boolean isUUID() {
        return false;
    }

    public boolean isAllColumns() {
        return this.allColumns;
    }

    @Override
    public int getColumnOrdinal() {
        return this.columnOrdinal;
    }

    public String getName() {
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    @Override
    public int compareTo(IQueryColumn other) {
        return Integer.compare(this.getColumnOrdinal(), other.getColumnOrdinal());
    }

    public IQueryColumn getQueryColumn() {
        return this.queryColumn;
    }

    @Override
    public String toString() {
        if (getTableContainer() != null) {
            return String.format("%s(%s)", getFunctionName(), getTableContainer().getTableNameOrAlias() + "." + getColumnName());
        }
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregationColumn)) return false;
        AggregationColumn that = (AggregationColumn) o;
        return isVisible() == that.isVisible()
                && isAllColumns() == that.isAllColumns()
                && getColumnOrdinal() == that.getColumnOrdinal()
                && getType() == that.getType()
                && Objects.equals(getAlias(), that.getAlias())
                && Objects.equals(getQueryColumn(), that.getQueryColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getAlias(), isVisible(), isAllColumns(), getQueryColumn(), getColumnOrdinal());
    }
}
