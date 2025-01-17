package com.gigaspaces.sql.datagateway.netty.query;

import java.util.Collections;
import java.util.List;

public class RowDescription {
    public static final RowDescription EMPTY = new RowDescription(Collections.emptyList());

    private final List<ColumnDescription> columns;

    public RowDescription(List<ColumnDescription> columns) {
        this.columns = columns;
    }

    public int getColumnsCount() {
        return columns.size();
    }

    public List<ColumnDescription> getColumns() {
        return Collections.unmodifiableList(columns);
    }
}
