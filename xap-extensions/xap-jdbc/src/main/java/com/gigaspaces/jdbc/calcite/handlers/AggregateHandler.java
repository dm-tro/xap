package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.GSAggregate;
import com.gigaspaces.jdbc.model.table.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public class AggregateHandler {
    private static AggregateHandler _instance;

    private AggregateHandler() {

    }

    public static AggregateHandler instance() {
        if (_instance == null) {
            _instance = new AggregateHandler();
        }
        return _instance;
    }

    public void apply(GSAggregate gsAggregate, QueryExecutor queryExecutor) {
        RelNode child = gsAggregate.getInput();
        if (child instanceof GSAggregate) {
            throw new UnsupportedOperationException("Unsupported yet!");
        }
        List<String> fields = child.getRowType().getFieldNames();
        for (ImmutableBitSet groupSet : gsAggregate.groupSets) {
            AtomicInteger columnCounter = new AtomicInteger();
            AtomicInteger groupByColumnCounter = new AtomicInteger();
            groupSet.forEach(index -> {
                String columnName = fields.get(index);
                TableContainer table = queryExecutor.isJoinQuery() ? queryExecutor.getTableByColumnIndex(index) : queryExecutor.getTableByColumnName(columnName);
                final IQueryColumn queryColumn = queryExecutor.getColumnByColumnIndex(index);
                if (queryColumn == null) {
                    table.addQueryColumnWithColumnOrdinal(columnName, null, true, columnCounter.getAndIncrement());
                }
                IQueryColumn groupByColumn = new ConcreteColumn(queryColumn == null ? columnName :
                        queryColumn.getName(), null, null, true, table, groupByColumnCounter.getAndIncrement());
                table.addGroupByColumns(groupByColumn);
            });
        }

        AtomicInteger columnCounter = new AtomicInteger();
        for (AggregateCall aggregateCall : gsAggregate.getAggCallList()) {
            AggregationFunctionType aggregationFunctionType = AggregationFunctionType.valueOf(aggregateCall.getAggregation().getName().toUpperCase());
            if (aggregateCall.getArgList().size() > 1) {
                throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                        + aggregateCall.getAggregation().getName().toUpperCase() + "()], expected 1 column but was " + aggregateCall.getArgList().size());
            }
            boolean allColumns = aggregateCall.getArgList().isEmpty();
            String column = allColumns ? "*" : fields.get(aggregateCall.getArgList().get(0));
            AggregationColumn aggregationColumn;
            if (allColumns) {
                queryExecutor.setAllColumnsSelected(true);
                if (aggregationFunctionType != AggregationFunctionType.COUNT) {
                    throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                            + aggregationFunctionType + "()], expected 1 column but was '*'");
                }
                aggregationColumn = new AggregationColumn(aggregationFunctionType, String.format("%s(%s)", aggregateCall.getAggregation().getName().toLowerCase(Locale.ROOT), column), null, true, true, columnCounter.getAndIncrement());
                queryExecutor.getTables().forEach(tableContainer -> tableContainer.addAggregationColumn(aggregationColumn));
                queryExecutor.getTables().forEach(t -> t.getAllColumnNames().forEach(columnName -> {
                    IQueryColumn qc = t.addQueryColumnWithoutOrdinal(columnName, null, false);
                    queryExecutor.addColumn(qc);
                }));
            } else {
                int index = aggregateCall.getArgList().get(0);
                final TableContainer table = queryExecutor.isJoinQuery() ? queryExecutor.getTableByColumnIndex(index) : queryExecutor.getTableByColumnName(column);
                final IQueryColumn queryColumn = queryExecutor.isJoinQuery() ? queryExecutor.getColumnByColumnIndex(index) : table.addQueryColumnWithoutOrdinal(column, null, false);
                queryExecutor.addColumn(queryColumn, false);
                aggregationColumn = new AggregationColumn(aggregationFunctionType, getFunctionAlias(aggregateCall, column), queryColumn, true, false, columnCounter.getAndIncrement());
                table.addAggregationColumn(aggregationColumn);
            }
            queryExecutor.addAggregationColumn(aggregationColumn);
        }
    }

    private String getFunctionAlias(AggregateCall call, String column) {
        return String.format("%s(%s)", call.getAggregation().getName().toLowerCase(Locale.ROOT), column);
    }
}
