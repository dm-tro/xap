package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.calcite.CalciteDefaults;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.explainplan.SubqueryExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.*;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryExecutor {
    private final List<IQueryColumn> projectedColumns = new ArrayList<>();
    private final List<TableContainer> tables = new ArrayList<>();
    private final Set<IQueryColumn> invisibleColumns = new HashSet<>();
    private final List<IQueryColumn> visibleColumns = new ArrayList<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private final IJSpace space;
    private final QueryExecutionConfig config;
    private final Object[] preparedValues;
    private boolean isAllColumnsSelected = false;
    private final LinkedList<Integer> fieldCountList = new LinkedList<>();
    private final List<CaseColumn> caseColumns = new ArrayList<>();
    private final List<IQueryColumn> groupByColumns = new ArrayList<>();


    public QueryExecutor(IJSpace space, QueryExecutionConfig config, Object[] preparedValues) {
        this.space = space;
        this.config = config;
        this.preparedValues = preparedValues;
    }

    public QueryExecutor(IJSpace space, Object[] preparedValues) {
        this(space, new QueryExecutionConfig().setCalcite(false), preparedValues);
    }

    public void addProjectedColumn(IQueryColumn column) {
        if (!column.isVisible()) {
            throw new IllegalStateException("Projected column must be visible");
        }
        this.projectedColumns.add(column);
    }

    public List<IQueryColumn> getProjectedColumns() {
        return projectedColumns;
    }

    public QueryResult execute() throws SQLException {
        if (tables.size() == 0) {
            if( hasOnlyFunctions() ) {
                List<IQueryColumn> visibleColumns = getVisibleColumns();
                TableRow row = TableRowFactory.createTableRowFromSpecificColumns(visibleColumns, Collections.emptyList(), Collections.emptyList());
                return new LocalSingleRowQueryResult(visibleColumns, row);
            } else {
                throw new SQLException("No tables has been detected");
            }
        }
        if (tables.size() == 1) { //Simple Query
            TableContainer singleTable = tables.get(0);
            QueryResult queryResult = singleTable.executeRead(config);
            final List<IQueryColumn> selectedColumns = getSelectedColumns();
            if (reIterateOverSingleTableResult(singleTable)) {
                if (config.isExplainPlan()) {
                    ExplainPlanQueryResult explainResult = ((ExplainPlanQueryResult) queryResult);
                    SubqueryExplainPlan subquery = new SubqueryExplainPlan(getSelectedColumns(),
                            config.getTempTableNameGenerator().generate(),
                            explainResult.getExplainPlanInfo(), null, Collections.unmodifiableList(getOrderColumns()),
                            Collections.unmodifiableList(getGroupByColumns()), false);
                    return new ExplainPlanQueryResult(getSelectedColumns(), subquery, singleTable);
                } else {
                    List<TableRow> rows = queryResult.getRows().stream().map(row -> TableRowFactory.createProjectedTableRow(row, this)).collect(Collectors.toList());
                    return new ConcreteQueryResult(selectedColumns, rows);
                }
            }
            return queryResult;
        }
        JoinQueryExecutor joinE = new JoinQueryExecutor(this);
        return joinE.execute();
    }

    private boolean reIterateOverSingleTableResult(TableContainer singleTable) {
        if (!config.isCalcite()) {
            return false;
        }
        if (getSelectedColumns().isEmpty()) {
            return false;
        }
        if (getSelectedColumns().size() != singleTable.getSelectedColumns().size()) {
            return true;
        }
        for (int i = 0; i < getSelectedColumns().size(); i++) {
            if (getSelectedColumns().get(i) != singleTable.getSelectedColumns().get(i)) {
                return true;
            }
        }
        return false;
    }

    public boolean isJoinQuery() {
        return tables.size() > 1;
    }

    private boolean hasOnlyFunctions() {
        if( !visibleColumns.isEmpty() ){
            for( IQueryColumn column : visibleColumns ){
                if( !( column instanceof FunctionCallColumn) ){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public List<TableContainer> getTables() {
        return tables;
    }

    public Set<IQueryColumn> getInvisibleColumns() {
        return invisibleColumns;
    }

    public List<IQueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    public Object[] getPreparedValues() {
        return preparedValues;
    }

    public boolean isAllColumnsSelected() {
        return isAllColumnsSelected;
    }

    public void setAllColumnsSelected(boolean isAllColumnsSelected) {
        this.isAllColumnsSelected = isAllColumnsSelected;
    }

    public List<AggregationColumn> getAggregationColumns() {
        return aggregationColumns;
    }

    public IJSpace getSpace() {
        return space;
    }

    public QueryExecutionConfig getConfig() {
        return config;
    }

    public void addColumn(IQueryColumn column, boolean isVisible) {
        if (isVisible) {
            visibleColumns.add(column);
        } else {
            invisibleColumns.add(column);
        }
    }

    public void addColumn(IQueryColumn column) {
        addColumn(column, column.isVisible());
    }

    public void addAggregationColumn(AggregationColumn aggregationColumn) {
        this.aggregationColumns.add(aggregationColumn);
    }

    public TableContainer getTableByColumnIndex(int columnIndex){
        initFieldCount();
        for (int i = 0; i < fieldCountList.size(); i++) {
            if(columnIndex < fieldCountList.get(i)){
                return getTables().get(i);
            }
        }
        throw new UnsupportedOperationException("");
    }

    public IQueryColumn getColumnByColumnIndex(int globalColumnIndex){
        initFieldCount();
        for (int i = 0; i < fieldCountList.size(); i++) {
            if(globalColumnIndex < fieldCountList.get(i)){
                int columnIndex = i == 0 ? globalColumnIndex : globalColumnIndex - fieldCountList.get(i - 1);
                return getTables().get(i).getVisibleColumns().get(columnIndex);
            }
        }
        return null;
    }

    private void initFieldCount(){
        if(fieldCountList.isEmpty() || fieldCountList.size() < tables.size()){
            fieldCountList.clear();
            for (TableContainer tableContainer: tables){
                tableContainer.fillAllColumns();
                int fieldCount =  tableContainer.getSelectedColumns().size();
                addFieldCount(fieldCount);
            }
        }
    }


    private void addFieldCount(int size) {
        int columnCount = fieldCountList.isEmpty() ?  size: fieldCountList.getLast() + size;
        fieldCountList.add(columnCount);
    }

    public void addCaseColumn(CaseColumn caseColumn) {
        this.caseColumns.add(caseColumn);
    }

    public TableContainer getTableByColumnName(String column) {
        TableContainer result = getTableByPhysicalColumnName(column);
        if (result != null) {
            return result;
        }
        for (TableContainer table : tables) {
            for (IQueryColumn queryColumn : table.getAllQueryColumns()) {
                if (column.equals(queryColumn.getName()) || column.equals(queryColumn.getAlias())) {
                    return table;
                }
            }
        }
        throw new ColumnNotFoundException("Column " + column + " wasn't found in any table");
    }

    public IQueryColumn getColumnByColumnName(String column) {
        for (TableContainer table : tables) {
            IQueryColumn result = table.getSelectedColumns().stream().filter(qc -> qc.getName().equals(column) || qc.getAlias().equals(column)).findFirst().orElse(null);
            if(result != null) {
                return result;
            }
        }
        return null;
    }

    public List<IQueryColumn> getSelectedColumns(){
        if (CalciteDefaults.isCalciteDriverPropertySet()) {
            return getProjectedColumns();
        }
        return Stream.concat(getVisibleColumns().stream(), getAggregationColumns().stream()).sorted().collect(Collectors.toList());
    }

    public List<IQueryColumn> getOrderColumns() {
        List<IQueryColumn> result = new ArrayList<>();
        tables.forEach(table -> result.addAll(table.getOrderColumns()));
        return result;
    }

    public List<IQueryColumn> getGroupByColumns() {
        return this.groupByColumns;
    }

    public void addGroupByColumn(IQueryColumn groupByColumn){
        this.groupByColumns.add(groupByColumn);
    }

    public TableContainer getTableByPhysicalColumnName(String name) {
        TableContainer toReturn = null;
        for (TableContainer tableContainer : getTables()) {
            if (tableContainer.hasColumn(name)) {
                if (toReturn == null) {
                    toReturn = tableContainer;
                } else {
                    throw new IllegalArgumentException("Ambiguous column name [" + name + "]");
                }
            }
        }
        return toReturn;
    }
}