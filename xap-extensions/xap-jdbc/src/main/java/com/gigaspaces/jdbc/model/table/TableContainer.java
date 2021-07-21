package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.calcite.CalciteDefaults;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.expression.Expression;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gigaspaces.jdbc.model.table.IQueryColumn.EMPTY_ORDINAL;

public abstract class TableContainer {
    private final List<IQueryColumn> projectedColumns = new ArrayList<>();
    private final List<OrderColumn> orderColumns = new ArrayList<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private final List<IQueryColumn> groupByColumns = new ArrayList<>();
    protected JoinInfo joinInfo;
    private boolean distinct;
    private Expression exprTree;
    private boolean allColumns = true;

    public void addProjectedColumn(IQueryColumn column) {
        if(!column.isVisible()) {
            throw new IllegalStateException("Projected column must be visible");
        }
        this.projectedColumns.add(column);
    }

    public List<IQueryColumn> getProjectedColumns() {
        return projectedColumns;
    }

    public abstract QueryResult executeRead(QueryExecutionConfig config) throws SQLException;

    public abstract IQueryColumn addQueryColumnWithColumnOrdinal(String columnName, String columnAlias, boolean isVisible, int columnOrdinal);

    public abstract List<IQueryColumn> getVisibleColumns();

    public abstract Set<IQueryColumn> getInvisibleColumns();

    public IQueryColumn addQueryColumnWithoutOrdinal(String columnName, String columnAlias, boolean isVisible){
        return addQueryColumnWithColumnOrdinal(columnName, columnAlias, isVisible, EMPTY_ORDINAL);
    }

    public List<IQueryColumn> getAllQueryColumns() {
        return Stream.concat(getVisibleColumns().stream(), getInvisibleColumns().stream()).collect(Collectors.toList());
    }

    public List<IQueryColumn> getSelectedColumns() {
        if (CalciteDefaults.isCalciteDriverPropertySet()) {
            return getProjectedColumns();
        }
        return Stream.concat(getVisibleColumns().stream(), getAggregationColumns().stream()).sorted().collect(Collectors.toList());
    }

    public abstract List<String> getAllColumnNames();

    public abstract String getTableNameOrAlias();

    public abstract void setLimit(Integer value);

    public abstract QueryTemplatePacket createQueryTemplatePacketWithRange(Range range);

    public abstract void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket);

    public abstract Object getColumnValue(String columnName, Object value) throws SQLException;

    public abstract TableContainer getJoinedTable();

    public abstract void setJoinedTable(TableContainer joinedTable);

    public abstract QueryResult getQueryResult();

    public abstract void setJoined(boolean joined);

    public abstract boolean isJoined();

    public abstract boolean hasColumn(String columnName);

    public boolean hasVisibleColumn(String columnName) {
        return getVisibleColumns().stream().map(IQueryColumn::getName).anyMatch(qcName -> Objects.equals(qcName,
                columnName));
    }

    public JoinInfo getJoinInfo() {
        return joinInfo;
    }

    public void setJoinInfo(JoinInfo joinInfo) {
        this.joinInfo = joinInfo;
    }

    public boolean checkJoinCondition() {
        if (joinInfo == null)
            return true;
        return joinInfo.checkJoinCondition();
    }

    public void setExpTree(Expression value) {
        this.exprTree = value;
    }

    public Expression getExprTree() {
        return exprTree;
    }

    public void addOrderColumns(OrderColumn orderColumn) {
        this.orderColumns.add(orderColumn);
    }

    public void addGroupByColumns(IQueryColumn groupByColumn) {
        this.groupByColumns.add(groupByColumn);
        if( !groupByColumn.isVisible() ) {
            this.getInvisibleColumns().add(groupByColumn);
        }
    }

    public List<IQueryColumn> getGroupByColumns() {
        return groupByColumns;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public void addAggregationColumn(AggregationColumn aggregationColumn) {
        this.aggregationColumns.add(aggregationColumn);
    }

    public List<AggregationColumn> getAggregationColumns() {
        return aggregationColumns;
    }

    public boolean hasGroupByColumns() {
        return !this.groupByColumns.isEmpty();
    }

    public boolean hasAggregationFunctions() {
        return !this.aggregationColumns.isEmpty();
    }

    public boolean hasOrderColumns() {
        return !this.orderColumns.isEmpty();
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public void fillAllColumns(){
        if(!getSelectedColumns().isEmpty()){
            return;
        }
        // select *
        List<String> columns = getAllColumnNames();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            IQueryColumn queryColumn = addQueryColumnWithoutOrdinal(column, null, true);
            addProjectedColumn(queryColumn);
        }
    }

    protected void validate(QueryExecutionConfig config) {
        if(!config.isCalcite())
            validateGroupBy();
        //TODO: block operation not supported -- see AggregationsUtil.convertAggregationResult
        if (hasAggregationFunctions() && hasOrderColumns()) {
            throw new IllegalArgumentException("Column [" + getOrderColumns().get(0).getAlias() + "] must appear in the " +
                    "GROUP BY clause or be used in an aggregate function");
        }
    }



    private void validateGroupBy() {
        if( hasAggregationFunctions()){
            List<IQueryColumn> visibleColumns = getVisibleColumns();
            if( visibleColumns.isEmpty() ){
                return;
            }
            List<String> groupByColumnNames = getGroupByColumns().stream().map(IQueryColumn::getName).collect(Collectors.toList());
            List<String> missingVisibleColumnNames = new ArrayList<>();
            for(IQueryColumn visibleColumn : visibleColumns){
                String visibleColumnName = visibleColumn.getName();
                if(!groupByColumnNames.contains(visibleColumnName)){
                    missingVisibleColumnNames.add( visibleColumnName );
                }
            }
            if(!missingVisibleColumnNames.isEmpty()){
                throw new IllegalArgumentException( ( missingVisibleColumnNames.size() == 1 ? "Column" : "Columns" ) + " " +
                        Arrays.toString( missingVisibleColumnNames.toArray( new String[0] ) ) + " must appear in the " +
                        "GROUP BY clause or be used in an aggregate function");
            }
        }
    }

    public boolean isAllColumns() {
        return allColumns;
    }

    public void setAllColumns(boolean allColumns) {
        this.allColumns = allColumns;
    }
}
