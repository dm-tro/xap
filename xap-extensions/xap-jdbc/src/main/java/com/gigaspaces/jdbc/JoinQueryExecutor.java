package com.gigaspaces.jdbc;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanResult;
import com.gigaspaces.jdbc.model.result.JoinTablesIterator;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JoinQueryExecutor {
    private final IJSpace space;
    private final List<TableContainer> tables;
    private final List<QueryColumn> queryColumns;
    private final QueryExecutionConfig config;

    public JoinQueryExecutor(List<TableContainer> tables, IJSpace space, List<QueryColumn> queryColumns, QueryExecutionConfig config) {
        this.tables = tables;
        this.space = space;
        this.queryColumns = queryColumns;
        this.config = config;
    }

    public QueryResult execute() {
        for (TableContainer table : tables) {
            try {
                table.executeRead(config);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }
        if(config.isExplainPlan())
            return explain();
        QueryResult res = new QueryResult(this.queryColumns);
        JoinTablesIterator joinTablesIterator = new JoinTablesIterator(tables);
        while (joinTablesIterator.hasNext()) {
            if(tables.stream().allMatch(TableContainer::checkJoinCondition))
                res.add(new TableRow(this.queryColumns));
        }
        return res;
    }

    private QueryResult explain() {
        TextReportFormatter formatter = new TextReportFormatter();
        formatter.line("Nested Loop Join");
        formatter.line("Select: " + String.join(", ",queryColumns.stream().map(QueryColumn::getName).collect(Collectors.toList())));
        formatter.indent();
        tables.forEach(t -> Arrays.stream(((ExplainPlanResult) t.getQueryResult()).getExplainPlanString().split("\n")).forEach(formatter::line));
        formatter.unindent();
        return new ExplainPlanResult(formatter.toString());
    }
}
