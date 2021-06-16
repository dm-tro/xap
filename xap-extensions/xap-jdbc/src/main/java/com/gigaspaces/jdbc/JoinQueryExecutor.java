package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.explainplan.JoinExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.*;
import com.gigaspaces.jdbc.model.table.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinQueryExecutor {
    private final List<TableContainer> tables;
    private final Set<IQueryColumn> invisibleColumns;
    private final List<IQueryColumn> visibleColumns;
    private final QueryExecutionConfig config;
    private final List<AggregationColumn> aggregationColumns;
    private final List<IQueryColumn> allQueryColumns;
    private final List<IQueryColumn> selectedQueryColumns;

    public JoinQueryExecutor(QueryExecutor queryExecutor) {
        this.tables = queryExecutor.getTables();
        this.invisibleColumns = queryExecutor.getInvisibleColumns();
        this.visibleColumns = queryExecutor.getVisibleColumns();
        this.config = queryExecutor.getConfig();
        this.config.setJoinUsed(true);
        this.aggregationColumns = queryExecutor.getAggregationColumns();
        this.allQueryColumns = Stream.concat(visibleColumns.stream(), invisibleColumns.stream()).collect(Collectors.toList());
        this.selectedQueryColumns = Stream.concat(this.visibleColumns.stream(), this.aggregationColumns.stream()).sorted().collect(Collectors.toList());
    }

    public QueryResult execute() {
        final List<OrderColumn> orderColumns = new ArrayList<>();
        final List<ConcreteColumn> groupByColumns = new ArrayList<>();
        for (TableContainer table : tables) {
            try {
                table.executeRead(config);
                orderColumns.addAll(table.getOrderColumns());
                groupByColumns.addAll(table.getGroupByColumns());
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }

        JoinTablesIterator joinTablesIterator = new JoinTablesIterator(tables);
        if(config.isExplainPlan()) {
            return explain(joinTablesIterator, orderColumns);
        }
        QueryResult res = new JoinQueryResult(this.selectedQueryColumns);
        while (joinTablesIterator.hasNext()) {
            if(tables.stream().allMatch(TableContainer::checkJoinCondition))
                res.addRow(TableRowFactory.createTableRowFromSpecificColumns(this.allQueryColumns, orderColumns,
                        groupByColumns));
        }
        if(!this.aggregationColumns.isEmpty()) {
            List<TableRow> aggregateRows = new ArrayList<>();
            aggregateRows.add(TableRowUtils.aggregate(res.getRows(), this.aggregationColumns));
            res.setRows(aggregateRows);
        }
        if(!groupByColumns.isEmpty()) {
            res.groupBy(); //group by the results at the client
        }
        if(!orderColumns.isEmpty()) {
            res.sort(); //sort the results at the client
        }

        return res;
    }

    private QueryResult explain(JoinTablesIterator joinTablesIterator, List<OrderColumn> orderColumns) {
        Stack<TableContainer> stack = new Stack<>();
        TableContainer current = joinTablesIterator.getStartingPoint();
        stack.push(current);
        while (current.getJoinedTable() != null){
            current = current.getJoinedTable();
            stack.push(current);
        }
        TableContainer first = stack.pop();
        TableContainer second = stack.pop();
        JoinExplainPlan joinExplainPlan = new JoinExplainPlan(first.getJoinInfo(), ((ExplainPlanQueryResult) first.getQueryResult()).getExplainPlanInfo(), ((ExplainPlanQueryResult) second.getQueryResult()).getExplainPlanInfo());
        TableContainer last = second;
        while (!stack.empty()) {
            TableContainer curr = stack.pop();
            joinExplainPlan = new JoinExplainPlan(last.getJoinInfo(), joinExplainPlan, ((ExplainPlanQueryResult) curr.getQueryResult()).getExplainPlanInfo());
            last = curr;
        }
        joinExplainPlan.setSelectColumns(visibleColumns.stream().map(IQueryColumn::toString).collect(Collectors.toList()));
        joinExplainPlan.setOrderColumns(orderColumns);
        return new ExplainPlanQueryResult(visibleColumns, joinExplainPlan, null);
    }
}
