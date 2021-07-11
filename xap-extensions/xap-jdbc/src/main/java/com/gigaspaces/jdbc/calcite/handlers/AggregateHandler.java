package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.GSAggregate;
import com.gigaspaces.jdbc.calcite.GSCalc;
import com.gigaspaces.jdbc.model.table.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static com.gigaspaces.jdbc.model.table.IQueryColumn.EMPTY_ORDINAL;

public class AggregateHandler {
    private static AggregateHandler _instance;

    public static AggregateHandler instance(){
        if(_instance == null){
            _instance = new AggregateHandler();
        }
        return _instance;
    }

    private AggregateHandler() {

    }

    public void apply(GSAggregate gsAggregate, QueryExecutor queryExecutor, boolean isRoot){
        RelNode child = gsAggregate.getInput();
        if(child instanceof GSAggregate){
            throw new UnsupportedOperationException("Unsupported yet!");
        }
        List<String> fields = child.getRowType().getFieldNames();
        if(isRoot) {
            for (ImmutableBitSet groupSet : gsAggregate.groupSets) {
                AtomicInteger columnCounter = new AtomicInteger();
                groupSet.forEach(bit -> {
                    String columnName = fields.get(bit);
                    TableContainer table = queryExecutor.getTableByColumnName(columnName);
                    ConcreteColumn groupByColumn = new ConcreteColumn(columnName, null, null, isVisibleColumn(queryExecutor, columnName), table, columnCounter.getAndIncrement());
                    table.addGroupByColumns(groupByColumn);
                });
            }
        }

        for (AggregateCall aggregateCall : gsAggregate.getAggCallList()) {
            AtomicInteger columnCounter = new AtomicInteger();
            AggregationFunctionType aggregationFunctionType = AggregationFunctionType.valueOf(aggregateCall.getAggregation().getName().toUpperCase());
            if(aggregateCall.getArgList().size() > 1){
                throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                        + aggregateCall.getAggregation().getName().toUpperCase() + "()], expected 1 column but was " + aggregateCall.getArgList().size());
            }
            boolean allColumns = aggregateCall.getArgList().isEmpty();
            String column = allColumns ? "*" : fields.get(aggregateCall.getArgList().get(0));
            AggregationColumn aggregationColumn;
            if(allColumns){
                queryExecutor.setAllColumnsSelected(true);
                if (aggregationFunctionType != AggregationFunctionType.COUNT) {
                    throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                            + aggregationFunctionType + "()], expected 1 column but was '*'");
                }
                aggregationColumn = new AggregationColumn(aggregationFunctionType, String.format("%s(%s)", aggregateCall.getAggregation().getName().toLowerCase(Locale.ROOT), column), null, true, true, isRoot ? columnCounter.getAndIncrement() : 0);
                queryExecutor.getTables().forEach(tableContainer -> tableContainer.addAggregationColumn(aggregationColumn));
                queryExecutor.getTables().forEach(t -> t.getAllColumnNames().forEach(columnName -> {
                    IQueryColumn qc = t.addQueryColumnWithoutOrdinal(columnName, null, false);
                    queryExecutor.addColumn(qc);
                }));
            }
            else{
                if(child instanceof GSCalc && queryExecutor.getTables().size() > 1){
                    int globalIndex = ((GSCalc) child).getProgram().getSourceField(aggregateCall.getArgList().get(0));
                    final TableContainer table = queryExecutor.getTableByColumnIndex(globalIndex);
                    IQueryColumn queryColumn = table.addQueryColumnWithoutOrdinal(queryExecutor.getColumnByColumnIndex(globalIndex).getName(), null, false);
                    queryExecutor.addColumn(queryColumn);
                    aggregationColumn = new AggregationColumn(aggregationFunctionType, String.format("%s(%s)", aggregateCall.getAggregation().getName().toLowerCase(Locale.ROOT), column), queryColumn, true, false, isRoot ? columnCounter.getAndIncrement() : EMPTY_ORDINAL);
                    table.addAggregationColumn(aggregationColumn);
                }
                else{
                    final TableContainer table = queryExecutor.getTableByColumnName(column);
                    IQueryColumn queryColumn = table.addQueryColumnWithoutOrdinal(column, null, false);
                    queryExecutor.addColumn(queryColumn);
                    aggregationColumn = new AggregationColumn(aggregationFunctionType, String.format("%s(%s)", aggregateCall.getAggregation().getName().toLowerCase(Locale.ROOT), column), queryColumn, true, false, isRoot ? columnCounter.getAndIncrement() : EMPTY_ORDINAL);
                    table.addAggregationColumn(aggregationColumn);
                }
            }
            if(isRoot) {
                queryExecutor.addAggregationColumn(aggregationColumn);
            }
        }
    }

    private boolean isVisibleColumn(QueryExecutor queryExecutor, String columnName) {
        return queryExecutor.getVisibleColumns().stream().anyMatch(queryColumn -> queryColumn.getAlias().equals(columnName));
    }
}
