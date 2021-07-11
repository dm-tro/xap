package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.*;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryExecutor {
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
    private int columnCounter = 0;
    private final List<ConcreteColumn> groupByColumns = new ArrayList<>();


    public QueryExecutor(IJSpace space, QueryExecutionConfig config, Object[] preparedValues) {
        this.space = space;
        this.config = config;
        this.preparedValues = preparedValues;
    }

    public QueryExecutor(IJSpace space, Object[] preparedValues) {
        this(space, new QueryExecutionConfig().setCalcite(false), preparedValues);
    }

    public QueryResult execute() throws SQLException {
        if (tables.size() == 0) {
            if( hasOnlyFunctions() ) {
                List<IQueryColumn> visibleColumns = getVisibleColumns();
                TableRow row = TableRowFactory.createTableRowFromSpecificColumns(visibleColumns, Collections.emptyList(), Collections.emptyList());
                return new LocalSingleRowQueryResult(visibleColumns, row);
            }
            else {
                throw new SQLException("No tables has been detected");
            }
        }
        if (tables.size() == 1) { //Simple Query
            TableContainer singleTable = tables.get(0);
            if(singleTable.hasGroupByColumns()){
                singleTable.getVisibleColumns().clear();
                singleTable.getVisibleColumns().addAll(singleTable.getGroupByColumns());
            }
            QueryResult queryResult =  singleTable.executeRead(config);
            queryResult.addCaseColumnsToResults(caseColumns);
            final List<IQueryColumn> selectedColumns = getSelectedColumns();
            if(!selectedColumns.isEmpty() && config.isCalcite()){
                List<TableRow> rows = queryResult.getRows().stream().map(row -> TableRowFactory.createProjectedTableRow(row, this)).collect(Collectors.toList());
                return new ConcreteQueryResult(selectedColumns, rows);
            }
            return queryResult;
        }
        JoinQueryExecutor joinE = new JoinQueryExecutor(this);
        QueryResult joinQueryResult = joinE.execute();
        joinQueryResult.addCaseColumnsToResults(caseColumns);
        return joinQueryResult;
    }

    public boolean isJoinQuery(){
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
            column.setColumnOrdinal(columnCounter++);
            visibleColumns.add(column);
        } else {
            invisibleColumns.add(column);
        }
    }

    public void addColumn(IQueryColumn column) {
        addColumn(column, column.isVisible());
    }

    public void addAggregationColumn(AggregationColumn aggregationColumn) {
        aggregationColumn.setColumnOrdinal(columnCounter++);
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
        throw new UnsupportedOperationException("");
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

    public TableContainer getTableByColumnName(String name) {
        TableContainer toReturn = null;
        for(TableContainer tableContainer : getTables()) {
            if(tableContainer.hasColumn(name)) {
                if (toReturn == null) {
                    toReturn = tableContainer;
                } else {
                    throw new IllegalArgumentException("Ambiguous column name [" + name + "]");
                }
            }
        }
        if(toReturn == null){
            throw new ColumnNotFoundException("Column " + name + " wasn't found in any table");
        }
        return toReturn;
    }

    public IQueryColumn getColumnByColumnName(String column) {
        TableContainer tableContainer = getTableByColumnName(column);
        return tableContainer.getAllQueryColumns().stream().filter(qc -> qc.getName().equals(column)).findFirst().orElse(null);
    }

    public List<IQueryColumn> getSelectedColumns(){
        return Stream.concat(getVisibleColumns().stream(), getAggregationColumns().stream()).sorted().collect(Collectors.toList());
    }

    public List<IQueryColumn> getOrderColumns() {
        List<IQueryColumn> result = new ArrayList<>();
        tables.forEach(table -> result.addAll(table.getOrderColumns()));
        return result;
    }

    public List<ConcreteColumn> getGroupByColumns() {
        return this.groupByColumns;
    }

    public void addGroupByColumn(ConcreteColumn groupByColumn){
        this.groupByColumns.add(groupByColumn);
    }
}
