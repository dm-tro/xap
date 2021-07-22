package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.GSAggregate;
import com.gigaspaces.jdbc.model.table.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Locale;

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
            groupSet.forEach(index -> {
                String columnName = fields.get(index);
                TableContainer table = queryExecutor.isJoinQuery() ? queryExecutor.getTableByColumnIndex(index) : queryExecutor.getTableByColumnName(columnName);
                final IQueryColumn queryColumn = queryExecutor.getColumnByColumnIndex(index);
                if (queryColumn == null) {
                    IQueryColumn qc = table.addQueryColumnWithoutOrdinal(columnName, null, true);
                    table.addProjectedColumn(qc);
                }
                IQueryColumn groupByColumn = new ConcreteColumn(queryColumn == null ? columnName :
                        queryColumn.getName(), null, null, true, table, -1);
                table.addGroupByColumns(groupByColumn);
            });
        }

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
                aggregationColumn = new AggregationColumn(aggregationFunctionType, aggregateCall.getName(), null,
                        true, true, -1);
                queryExecutor.getTables().forEach(tableContainer -> tableContainer.addAggregationColumn(aggregationColumn));
                queryExecutor.getTables().forEach(t -> t.getAllColumnNames().forEach(columnName -> {
                    IQueryColumn qc = t.addQueryColumnWithoutOrdinal(columnName, null, false);
                    queryExecutor.addColumn(qc);
                    queryExecutor.addProjectedColumn(qc);
                }));
            } else {
                int index = aggregateCall.getArgList().get(0);
                final TableContainer table = queryExecutor.isJoinQuery() ? queryExecutor.getTableByColumnIndex(index) : queryExecutor.getTableByColumnName(column);
                final IQueryColumn queryColumn = queryExecutor.isJoinQuery() ? queryExecutor.getColumnByColumnIndex(index) : table.addQueryColumnWithoutOrdinal(column, null, false);
                queryExecutor.addColumn(queryColumn, false);
                aggregationColumn = new AggregationColumn(aggregationFunctionType, aggregateCall.getName(), queryColumn, true
                        , false, -1);
                table.addAggregationColumn(aggregationColumn);
                table.addProjectedColumn(aggregationColumn);
            }
        }
    }

    private String getFunctionAlias(AggregateCall call, String column) {
        return String.format("%s(%s)", call.getAggregation().getName().toLowerCase(Locale.ROOT), column);
    }
}
