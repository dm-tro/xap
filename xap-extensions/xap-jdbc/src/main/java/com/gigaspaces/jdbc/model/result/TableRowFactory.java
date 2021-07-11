package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.model.table.*;

import java.util.List;

public class TableRowFactory {

    public static TableRow createTableRowFromSpecificColumns(List<IQueryColumn> columns,
                                                             List<OrderColumn> orderColumns,
                                                             List<IQueryColumn> groupByColumns) {
        return new TableRow(columns, orderColumns, groupByColumns);
    }

    public static TableRow createTableRowFromIEntryPacket(IEntryPacket entryPacket, ConcreteTableContainer tableContainer) {
        return new TableRow(entryPacket, tableContainer);
    }

    public static TableRow createProjectedTableRow(TableRow row, TempTableContainer tempTableContainer) {
        return new TableRow(row, tempTableContainer);
    }

    public static TableRow createProjectedTableRow(TableRow row, QueryExecutor queryExecutor) {
        return new TableRow(row, queryExecutor);
    }
}
