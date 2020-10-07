/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.jdbc;

import com.gigaspaces.client.transaction.ITransactionManagerProvider;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.gigaspaces.security.AccessDeniedException;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.service.SecurityContext;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceContextHelper;
import com.j_spaces.core.admin.SpaceRuntimeInfo;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.jdbc.batching.BatchResponsePacket;
import com.j_spaces.jdbc.builder.QueryEntryPacket;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.driver.GPreparedStatement.PreparedValuesCollection;
import com.j_spaces.jdbc.executor.CollocatedJoinedQueryExecutor;
import com.j_spaces.jdbc.executor.JoinedQueryExecutor;
import com.j_spaces.jdbc.executor.QueryExecutor;
import com.j_spaces.jdbc.parser.*;
import com.j_spaces.jdbc.query.*;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.TransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.*;

import static com.j_spaces.jdbc.Join.JoinType.INNER;


/**
 * This class handles the SELECT query logic.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class SelectQuery extends AbstractDMLQuery {

    /**
     * SYSTABLES constants
     */
    private static final String SYSTABLES = "SYSTABLES";
    private static final String SYSTABLES_TABLENAME = "TABLENAME";

    private ArrayList<OrderColumn> orderColumns = null;
    private ArrayList<SelectColumn> groupColumn = null;
    private boolean isAggFunction = false;
    private boolean forUpdate = false; //is this select for an update.
    private boolean isAddAbsentCol = false;

    // Distinct query
    private boolean isDistinct = false;

    //Aggregation API settings
    private static final boolean useAggregationsApi = Boolean.parseBoolean(System.getProperty("com.gigaspaces.query.useAggregationsApi", "true"));
    protected AggregationSet _aggregationSet;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);
    private boolean isSelectAll;
    private List<Join> joins;
    private boolean flattenResults;

    public SelectQuery() {
        super();
    }


    public void addTableWithAlias(Object table, String alias) {
        if (table instanceof String) {
            super.addTableWithAlias((String) table, alias);
        } else if (table instanceof  SelectQuery){
            SelectQuery query = (SelectQuery) table;
            QueryTableData queryTableData = super.addTableWithAlias(alias, null);
            queryTableData.setSubQuery(query);
        }
    }

    public void setJoins(List<Join> joins) {
        this.joins = joins;
    }

    /**
     * @return list of aggregated functions in query
     */
    private ArrayList<SelectColumn> getAggregateFunc() {

        List<SelectColumn> aList = getQueryColumns();
        if (aList == null || aList.isEmpty())
            return new ArrayList<SelectColumn>(0);

        ArrayList<SelectColumn> aFuncList = new ArrayList<SelectColumn>(aList.size());
        for (int i = 0; i < aList.size(); i++) {
            SelectColumn col = aList.get(i);

            if (col.isAggregatedFunction())
                aFuncList.add(col);
        }

        return aFuncList;
    }

    public void addAbsentCol() {
        this.isAddAbsentCol = true;
    }

    public void setAggFunction(boolean flag) {
        this.isAggFunction = flag;
    }

    public boolean isAggFunction() {
        return this.isAggFunction;
    }

    private boolean isCount() {
        List<SelectColumn> aList = getQueryColumns();
        if (aList == null || aList.isEmpty())
            return false;

        return aList.get(0).isCount();
    }

    /**
     * Execute the query
     */
    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        IQueryResultSet<IEntryPacket> entries = null;
        ResponsePacket packet = new ResponsePacket();
        try {
            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.READ, getTableName());
            }

            if (getSession() != null && getSession().getModifiers() != null)
                validateCommonJavaTypeOnDocumentOrStringReturnProperties();

            // handle select for update
            txn = startTransaction(space, txn);

            // prepare - bind the query parameters            
            prepare(space, txn);

            if (useAggregationApi(txn))
                createProjectionTemplate();
            /***************** Read the entries ****************/

            if (SYSTABLES.equals(getTableName())) {
                return executeSysTablesQuery(space, packet);
            }


            // Execute the query - read the entries from space
            // No where clause

            if (isJoined()) {
                boolean collJoin = false;//isCollocatedJoin();
                _executor = collJoin ? new CollocatedJoinedQueryExecutor(this) : new JoinedQueryExecutor(this);

                entries = executeJoinedQuery(space, txn);
            } else if (expTree == null) {
                _executor = new QueryExecutor(this);

                if (isCount() && !isGroupBy()) {
                    //  Handle count
                    return executeCountAll(space, txn);
                } else if (useAggregationApi(txn)) {
                    //  Handle aggregated functions
                    _aggregationSet = AggregationsUtil.createAggregationSet(this, getRownumLimit());
                }

                entries = executeEmptyQuery(space, txn, entries);

            } else {// select with expression
                _executor = new QueryExecutor(this);

                // Handle composite queries
                if (expTree.getTemplate() == null || expTree.getTemplate().isComplex()) {
                    entries = executeQuery(space, txn);
                }
                // Handle queries that won't return anything
                else if (expTree.getTemplate().isAlwaysEmpty()) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Logical error - query is always empty - fix your SQL syntax");
                    }

                    // Build only queries can't be handled
                    if (isBuildOnly())
                        throw new SQLException("Logical error - query is always empty - fix your SQL syntax");

                    entries = new ArrayListResult();

                } else {
                    // Handle build only queries
                    // return immediately
                    // no processing is needed
                    if (isBuildOnly()) {
                        entries = new ArrayListResult();
                        build();
                        entries.add(expTree.getTemplate());
                        packet.setResultSet(entries);
                        return packet;
                    }

                    // Check if can be executed as one count query
                    if (isCount() && !isGroupBy() && !isDistinct() && getAggregateFunc().size() == 1) {
                        return executeCount(expTree.getTemplate(), space, txn);
                    }
                    // Execute the read query
                    entries = executeQuery(space, txn);
                }
            }

            /********************** done reading the entries ********************/
            /*** by now, entries is a set of ExternalEntries or JoinedEntries ***/


            //  Handle empty result
            if (entries.isEmpty()) {
                return emptyResult(entries);
            }

            if (isConvertResultToArray())
                addDynamicSelectColumns(entries);

            // Create the projected indices
            createProjectionIndices(entries);


            // Handle group by
            if (!useAggregationApi(txn)) {
                if (isGroupBy()) {
                    entries = groupBy(entries);
                } else if (isAggFunction()) // Handle aggregation
                {
                    entries = aggregate(entries);
                }
            }

            //Handle distinct quantifier
            if (isDistinct()) {
                entries = _executor.filterDistinctEntries(entries);
            }

            //if order by is relevant, this is the place to order.
            //let's start by finding that column's position
            if (isOrderBy() && (!useAggregationApi(txn) || getGroupColumn() != null)) {
                orderBy(entries);
            }

            //Handle rownum
            filterByRownum(entries);


            prepareResult(packet, entries);
        } catch (AccessDeniedException e) {
            throw e;
        } catch (BatchQueryException e) {
            throw e;
        } catch (Exception e) {
            if (_logger.isDebugEnabled()) {
                _logger.error(e.getMessage(), e);
            }
            throw new SQLException("Select failed; Cause: " + e, "GSP", -120, e);
        }
        return packet;
    }
    private boolean isCollocatedJoin() {
        List<String> refTableNames = Arrays.asList(System.getProperty("com.gs.sql.refTables").split(","));
        List<QueryTableData> shardedTables = new ArrayList<>();
        List<QueryTableData> refTables = new ArrayList<>();

        List<QueryTableData> tablesInQuery = getJoinTables();
        for (QueryTableData queryTableData : tablesInQuery) {
            if (refTableNames.contains(queryTableData.getTableName())) {
                refTables.add(queryTableData);
            } else {
                shardedTables.add(queryTableData);
            }
        }

        if (shardedTables.size() > 1) {
            return isJoinOnRouting(refTableNames);
        }
        return true;
    }

    private boolean isJoinOnRouting(List<String> refTableNames) {
        if (joins == null) return true;

        for (Join join : joins) {
            if (join.getSubQuery() == null) {
                if (!refTableNames.contains(join.getTableName())) { // if not ref table
                    if (!isJoinOnRouting(join.getOnExpression(), (SelectQuery)join.getSubQuery())) return false; // if ON is not on routing key
                }
            } else if (join.getSubQuery() instanceof SelectQuery) {
                SelectQuery q = (SelectQuery) join.getSubQuery();
                if (!isJoinOnRouting(join.getOnExpression(), q)) return false;
            } else {
                throw new UnsupportedOperationException(); //TODO
            }
        }
        return true;
    }

    private boolean isJoinOnRouting(ExpNode expNode, SelectQuery subQuery) {
        if (expNode instanceof AndNode) {
            return isJoinOnRouting(expNode.getRightChild(), subQuery) && isJoinOnRouting(expNode.getLeftChild(), subQuery);
        } else if (expNode instanceof EqualNode) {
            ExpNode left = expNode.getLeftChild();
            ExpNode right = expNode.getRightChild();
            if (left instanceof ColumnNode && right instanceof ColumnNode) {
                ColumnNode leftNode = (ColumnNode) left;
                ColumnNode rightNode = (ColumnNode) right;

                String leftNodeJoinOn = leftNode.getColumnData().getColumnName();
                ITypeDesc leftNodeTypeDesc = leftNode.getColumnData().getColumnTableData().getTypeDesc();

                String rightNodeJoinOn = rightNode.getColumnData().getColumnName();
                ITypeDesc rightNodeTypeDesc = rightNode.getColumnData().getColumnTableData().getTypeDesc();
                if (rightNodeTypeDesc == null) {
                    rightNodeTypeDesc = subQuery.getQueryColumns().stream().filter(qc -> qc.getName().equals(rightNodeJoinOn)).findFirst().get().getColumnTableData().getTypeDesc();
                }


                boolean isLeftNodeJoinOnRouting = leftNodeJoinOn.equals(leftNodeTypeDesc.getRoutingPropertyName());
                boolean isRightNodeJoinOnRouting = rightNodeJoinOn.equals(rightNodeTypeDesc.getRoutingPropertyName());
                return isLeftNodeJoinOnRouting && isRightNodeJoinOnRouting;
            } else {
                throw new UnsupportedOperationException("Unsupported left and right nodes type"); //TODO
            }
        } else {
            throw new UnsupportedOperationException("Unsupported for non AndNode and EqualNode"); //TODO
        }
    }

    private List<Join> getJoins() {
        if (joins == null) return Collections.emptyList();

        List<Join> allJoins = new ArrayList<>(joins);
        for (Join join : joins) {
            if (join.getSubQuery() != null && join.getSubQuery() instanceof SelectQuery) {
                allJoins.addAll(((SelectQuery)join.getSubQuery()).getJoins());
            }
        }

        return allJoins;
    }

    private List<QueryTableData> getJoinTables() {
        List<QueryTableData> list = new ArrayList<>();

        for (QueryTableData tablesDatum : _tablesData) {
            if (tablesDatum.getSubQuery() == null) {
                list.add(tablesDatum);
            } else {
                if (tablesDatum.getSubQuery() instanceof SelectQuery) {
                    list.addAll(((SelectQuery)tablesDatum.getSubQuery()).getJoinTables());
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }
        return list;
    }

    private boolean isCollocatedJoinOld() {
        List<String> refTableNames = Arrays.asList(System.getProperty("com.gs.sql.refTables").split(","));

        List<QueryTableData> shardedTables = new ArrayList<>();
        List<QueryTableData> refTables = new ArrayList<>();

        _tablesData.forEach(queryTableData -> {
            if (refTableNames.contains(queryTableData.getTableName())) {
                refTables.add(queryTableData);
            } else {
                shardedTables.add(queryTableData);
            }
        });

        if (shardedTables.size() > 1) {
            for (Join join : joins) {
                ColumnNode leftNode = (ColumnNode) join.getOnExpression().getLeftChild();
                ColumnNode rightNode = (ColumnNode) join.getOnExpression().getRightChild();

                String leftNodeJoinOn = leftNode.getColumnData().getColumnName();
                String rightNodeJoinOn = rightNode.getColumnData().getColumnName();

                boolean isLeftNodeJoinOnRouting = leftNodeJoinOn.equals(leftNode.getColumnData().getColumnTableData().getTypeDesc().getRoutingPropertyName());
                if (!isLeftNodeJoinOnRouting) return false;

                if (join.getSubQuery() == null) {
                    if (!rightNodeJoinOn.equals(rightNode.getColumnData().getColumnTableData().getTypeDesc().getRoutingPropertyName())) {
                        return false;
                    }
                } else {
                    SelectQuery subQuery = (SelectQuery) join.getSubQuery();
                    if (!rightNodeJoinOn.equals(subQuery.getTableData().getTypeDesc().getRoutingPropertyName())) {
                        return false;
                    }
                    if (!subQuery.isCollocatedJoin()) {
                        return false;
                    }
                }
            }
        }

        return true;
    }


    /**
     * @throws SQLException
     */
    private IQueryResultSet<IEntryPacket> aggregate(IQueryResultSet<IEntryPacket> entries) throws SQLException {
        IEntryPacket aggregation = _executor.aggregate(entries);
        entries = new ProjectedResultSet();
        entries.add(aggregation);
        return entries;
    }

    /**
     * @throws SQLException
     */
    private IQueryResultSet<IEntryPacket> groupBy(IQueryResultSet<IEntryPacket> entries) throws SQLException {
        return _executor.groupBy(entries, groupColumn);
    }


    /**
     * @param packet
     * @throws SQLException
     */
    private void prepareResult(ResponsePacket packet, IQueryResultSet<IEntryPacket> entries) throws SQLException {
        // Check if entries should be converted to array structure
        if (isConvertResultToArray()) {
            ResultEntry result;

            //  Handle aggregation separately
            // TODO unite with the rest of queries
            if (isAggFunction()) {
                result = buildAggregationResult(entries);
            } else {
                //  Create the result arrays - fieldValues
                result = _executor.convertEntriesToResultArrays(entries);

            }
            packet.setResultEntry(result);
        } else {
            packet.setResultSet(flattenResults ? _executor.flattenEntryPackets(entries) : entries);
        }
    }

    /**
     * @throws SQLException
     *
     */
    private void orderBy(IQueryResultSet<IEntryPacket> entries) throws SQLException {
        _executor.orderBy(entries, orderColumns);
    }

    private IQueryResultSet<IEntryPacket> executeQuery(ISpaceProxy space, Transaction txn) throws SQLException {
        if (useAggregationApi(txn) && expTree.getTemplate() != null) {
            //  Handle aggregated functions
            _aggregationSet = AggregationsUtil.createAggregationSet(this, getRownumLimit());
            expTree.getTemplate().setAggregationSet(_aggregationSet);
        }
        return _executor.execute(space, txn, getReadModifier(), getEntriesLimit());
    }

    /**
     * Returns the upper limit to number of entries that the query should return
     */
    private int getEntriesLimit() {
        // Check for complex queries that require that all data is fetched
        if (isOrderBy() || isGroupBy() || isDistinct() || isAggFunction()) {
            return Integer.MAX_VALUE;
        }
        return super.getRownumLimit();
    }

    /**
     * Handles select for update queries Creates a local transaction if none is specified
     */
    private Transaction startTransaction(IJSpace space, Transaction txn)
            throws LeaseDeniedException, RemoteException, TransactionException {
        if (forUpdate && txn == null) {
            ITransactionManagerProvider managerProvider = getSession().getQueryHandler().getTransactionManagerProvider();
            //if this is a select for update, we must use a transaction,
            //so in case there wasn't any, we create one
            txn = (TransactionFactory.create(managerProvider.getTransactionManager(),
                    QueryProcessor.getDefaultConfig().getTransactionTimeout() * 100)).transaction;
            this.getSession().setTransaction(txn);
        }
        return txn;
    }

    /**
     * Execute query on SYS_TABLES Returns a list of classes in space
     */
    private ResponsePacket executeSysTablesQuery(IJSpace space,
                                                 ResponsePacket packet) throws RemoteException {

        Object[][] fieldValues;
        Object classList[] = SQLUtil.getAdmin(space).getRuntimeInfo().m_ClassNames.toArray();

        String[] fieldNames;

        fieldNames = new String[1];
        fieldNames[0] = SYSTABLES_TABLENAME;
        fieldValues = new Object[classList.length][1];
        for (int i = 0; i < classList.length; i++) {
            fieldValues[i][0] = classList[i];
        }

        ResultEntry result = new ResultEntry(
                fieldNames,
                new String[]{getQueryColumns().get(0).getAlias()},
                new String[]{SYSTABLES},
                fieldValues);

        packet.setResultEntry(result);
        return packet;
    }

    /**
     * Handle read query - check if it is a read query or a readMultiple and execute it
     *
     * @return the query result
     */
    private ResponsePacket executeCount(QueryTemplatePacket template, IJSpace space,
                                        Transaction txn) throws SQLException {
        try {
            template.setRouting(getRouting());
            template.setExplainPlan(getExplainPlan());
            int count = space.count(template, txn, getReadModifier());

            ResponsePacket response = new ResponsePacket();

            Object[][] values = new Object[1][1];
            values[0][0] = count;

            // COUNT's column tablename is an empty String
            ResultEntry result = new ResultEntry(
                    new String[]{getCountColumnName()},
                    new String[]{getCountColumnLabel()},
                    new String[]{""},
                    values);

            response.setResultEntry(result);

            return response;
        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }
            throw new SQLException("Failed to execute count: " + e, "GSP", -111);
        }

    }


    /**
     * Gets the count column name. (e.g. COUNT(*), COUNT(id) or alias)
     */
    private String getCountColumnName() {
        return getQueryColumns().get(0).toString();
    }

    /**
     * Gets the count column alias, if no alias is set, gets the column name
     */
    private String getCountColumnLabel() {
        return getQueryColumns().get(0).getAlias();
    }

    /**
     * Build result object from aggregation
     */
    private ResultEntry buildAggregationResult(IQueryResultSet<IEntryPacket> entries) {
        int i = 0;
        String[] fieldNames = null;
        Object[][] fieldValues = null;
        for (IEntryPacket packet : entries) {
            QueryEntryPacket entry = (QueryEntryPacket) packet;

            if (fieldNames == null) {
                fieldNames = entry.getFieldNames();
                fieldValues = new Object[entries.size()][fieldNames.length];
            }

            fieldValues[i++] = entry.getFieldValues();

        }

        // Aggregation table names are always empty strings
        String[] tableNames = new String[fieldNames.length];
        for (int j = 0; j < tableNames.length; j++)
            tableNames[j] = "";

        // Set column labels
        // Alias overrides aggregation column names
        String[] columnLabels = new String[fieldNames.length];
        int fieldIndex = 0;
        for (SelectColumn column : getQueryColumns()) {
            if (column.isVisible())
                columnLabels[fieldIndex++] = column.getAlias();
        }

        ResultEntry result = new ResultEntry(
                fieldNames,
                columnLabels,
                tableNames,
                fieldValues);

        return result;
    }

    /**
     * Called when the query result is empty Creates an empty result object.
     *
     * @return ResponsePacket that contains an empty result
     */
    private ResponsePacket emptyResult(IQueryResultSet<IEntryPacket> entries) {
        // Set an empty result metadata
        String[] columnLabels;
        String[] tableNames;
        String[] fieldNames;
        Object[][] fieldValues;

        // COUNT fucntion has only 1 column with a blank table
        if (isCount()) {
            fieldNames = new String[]{getCountColumnName()};
            columnLabels = new String[]{getCountColumnLabel()};
            tableNames = new String[]{""};
            fieldValues = new Object[][]{{Integer.valueOf(0)}};
        } else {

            int numOfColumns = getQueryColumns().size();
            ArrayList<String> columnNamesList = new ArrayList<String>(numOfColumns);
            ArrayList<String> columnLabelsList = new ArrayList<String>(numOfColumns);
            ArrayList<String> tableNamesList = new ArrayList<String>(numOfColumns);

            // Gather metadata for visible columns
            for (SelectColumn resultColumn : getQueryColumns()) {
                if (resultColumn.isVisible()) {
                    columnNamesList.add(resultColumn.toString());
                    columnLabelsList.add(resultColumn.getAlias());
                    tableNamesList.add(resultColumn.getColumnTableData().getTableName());
                }
            }

            fieldNames = columnNamesList.toArray(new String[columnNamesList.size()]);
            columnLabels = columnLabelsList.toArray(new String[columnLabelsList.size()]);
            tableNames = tableNamesList.toArray(new String[tableNamesList.size()]);

            fieldValues = new Object[0][0];
        }

        ResultEntry result = new ResultEntry(
                fieldNames,
                columnLabels,
                tableNames,
                fieldValues);

        ResponsePacket packet = new ResponsePacket();
        packet.setResultEntry(result);
        packet.setResultSet(entries);

        return packet;
    }


    /**
     * Execute special simplified count for queries without where clause.
     */
    private ResponsePacket executeCountAll(IJSpace space, Transaction txn) throws RemoteException, TransactionException,
            UnusableEntryException {
        ResponsePacket packet = new ResponsePacket();

        // GS-7406: In embedded QP, a security check needs to be done since interception was skipped.
        // call space API and don't go through runtimeInfo which is currently not secured
        boolean embeddedQpNeedsSecurityCheck = space.isSecured() && getSecurityInterceptor() == null;

        Integer count = 0;
        if (((ISpaceProxy) space).isClustered() || embeddedQpNeedsSecurityCheck) {
            QueryTemplatePacket template = new QueryTemplatePacket(getTableData(), _queryResultType);
            template.setRouting(getRouting());
            count = space.count(template, txn);
        } else {
            // Optimized solution using Runtimeinfo for single proxy
            // Get class hierarchy count
            try {
                SpaceRuntimeInfo info = SQLUtil.getAdmin(space).getRuntimeInfo(getTableName());
                // Add subclasses count to the total count
                for (int entryCount : info.m_NumOFEntries) {
                    count += entryCount;
                }
            } catch (IllegalArgumentException ex) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Trying to count single space when the metadata is not available.", ex);
                }
                // Workaround in case the type was not introduced yet.
            }
        }

        // COUNT's column tablename is always an empty String
        ResultEntry result = new ResultEntry(
                new String[]{getCountColumnName()},
                new String[]{getCountColumnLabel()},
                new String[]{""},
                new Object[][]{{count}});

        packet.setResultEntry(result);

        return packet;
    }


    /**
     * Return a cloned SelectQuery
     */
    @Override
    public SelectQuery clone() {
        SelectQuery query = new SelectQuery();
        query.tables = this.tables;
        query._tablesData = _tablesData;
        query.rownum = (RowNumNode) (this.rownum == null ? null : rownum.clone());
        query.orderColumns = this.orderColumns;
        query.groupColumn = this.groupColumn;
        query.isPrepared = this.isPrepared;
        query.forUpdate = this.forUpdate;
        query.isAggFunction = this.isAggFunction;
        query.isDistinct = isDistinct;
        query.setRouting(this.getRouting());
        query.setProjectionTemplate(this.getProjectionTemplate());
        query.setContainsSubQueries(this.containsSubQueries());
        query.isSelectAll = this.isSelectAll;


        int numOfColumns = 0;
        for (SelectColumn col : this.getQueryColumns()) {
            if (!col.isDynamic()) {
                numOfColumns++;
            }
        }

        query.queryColumns = new ArrayList(numOfColumns);
        if(numOfColumns != 0) {
            for (SelectColumn col : this.getQueryColumns()) {
                if (!col.isDynamic()) {
                    query.queryColumns.add(col);
                }
            }
        }

        if (this.getExpTree() != null)
            query.setExpTree((ExpNode) this.getExpTree().clone()); //clone all the tree.
        return query;
    }

    /**
     * Add a column to the list of columns.
     *
     * @param column the column to add
     */
    public void addColumn(SelectColumn column) {
        if (queryColumns == null)
            queryColumns = new ArrayList();
        queryColumns.add(column);
    }


    /**
     * Sets the order column.
     */
    public void setOrderColumns(ArrayList<OrderColumn> ordCol) {
        this.orderColumns = ordCol;
    }

    /**
     * Sets the order column.
     */
    public void setGroupColumn(ArrayList<SelectColumn> groupColumnList) {
        this.groupColumn = groupColumnList;
    }

    public ArrayList<SelectColumn> getGroupColumn() {
        return this.groupColumn;
    }


    private boolean isGroupBy() {
        return this.groupColumn == null ? false : true;
    }

    /**
     * @return
     */
    private boolean isOrderBy() {
        return orderColumns != null && !orderColumns.isEmpty();
    }

    /**
     * Mark this select as a SELECT FOR UPDATE.
     */
    public void setForUpdate(boolean forUpdate) {
        this.forUpdate = forUpdate;
    }

    /**
     * This method pre-validates the query, in terms of selected tables and columns.
     */
    @Override
    public void validateQuery(ISpaceProxy space) throws SQLException {
        if (getTablesNames().contains(SYSTABLES)) {
            return;
        }

        applyJoinsIfNeeded();

        super.validateQuery(space);

        // if this query is used to create a notify template
        // perform specific validation
        if (isBuildOnly())
            validateNotifyQuery();

        validateAndPrepareSelectColumns();
        // set order column info
        if (isOrderBy()) {
            for (SelectColumn orderCol : orderColumns) {
                orderCol.createColumnData(this);
            }
        }

        if (isGroupBy()) {
            for (SelectColumn groupbyCol : groupColumn) {
                groupbyCol.createColumnData(this);
            }
        }

        validateCommonJavaTypeOnDocumentOrStringReturnProperties();
    }

    private void applyJoinsIfNeeded() throws SQLException {
        if (joins != null) {
            for (Join join : joins) {
                QueryTableData table = addTableWithAlias(join.getTableName(), join.getAlias());
                table.setJoinType(join.getJoinType());
                if (join.getSubQuery() != null) {
                    table.setSubQuery(join.getSubQuery());
                }
                setExpTree(join.applyOnExpression(getExpTree()));
            }
        }
    }

    private void validateCommonJavaTypeOnDocumentOrStringReturnProperties() {
        if (Modifiers.contains(getReadModifier(), Modifiers.RETURN_STRING_PROPERTIES) || (Modifiers.contains(getReadModifier(), Modifiers.RETURN_DOCUMENT_PROPERTIES))) {
            if (isOrderBy()) {
                for (SelectColumn column : orderColumns) {
                    if (!isCommonJavaType( column )) {
                        QueryColumnData columnData = column.getColumnData();
                        int propertyIndex = columnData.getColumnIndexInTable();
                        throw new UnsupportedOperationException(
                                "ORDER BY can only be performed by specifying java common types while provided type is: "
                                        + columnData.getColumnTableData().getTypeDesc()
                                        .getPropertiesTypes()[propertyIndex]);
                    }
                }
            }
            if (isGroupBy()) {
                for (SelectColumn column : groupColumn) {
                    if (!isCommonJavaType( column )) {
                        QueryColumnData columnData = column.getColumnData();
                        int propertyIndex = columnData.getColumnIndexInTable();
                        throw new UnsupportedOperationException(
                                "GROUP BY can only be performed by specifying java common types while provided type is: "
                                        + columnData.getColumnTableData().getTypeDesc().getPropertiesTypes()[propertyIndex]);
                    }
                }
            }
        }
    }

    //fix for GS-13451, GS-13537
    private static boolean isCommonJavaType(SelectColumn column){
        QueryColumnData columnData = column.getColumnData();
        int propertyIndex = columnData.getColumnIndexInTable();

        String columnPath = columnData.getColumnPath();

        boolean commonJavaType = false;

        //if path is not simple, i.e. has delimiters
        //for example possible value here can be: teamMemberKey.contractMonth
        if( columnPath.contains( "." ) ) {
            try {
                Class<?> type = getFieldClassType(columnData);
                commonJavaType = ReflectionUtils.isCommonJavaType(type) || type.isEnum();
            }
            catch( NoSuchFieldException e ){
                if( _logger.isWarnEnabled()){
                    _logger.warn("Field [" + columnPath + "] does not exist", e );
                }
            }
            catch( Exception e ){
                if( _logger.isErrorEnabled()){
                    _logger.error("Failed verifying common Java type for [" + columnPath + "]", e );
                }
            }
        }
        else {
            PropertyInfo propertyInfo =
                    columnData.getColumnTableData().getTypeDesc().getProperties()[propertyIndex];
            commonJavaType = propertyInfo.isCommonJavaType() ||
                    ( propertyInfo.getType() != null && propertyInfo.getType().isEnum() );
        }

        return commonJavaType;
    }

    //fix for GS-13451
    private static Class<?> getFieldClassType( QueryColumnData columnData ) throws NoSuchFieldException{

        //for example columnPath has value "teamMemberKey.contractMonth"
        String columnPath = columnData.getColumnPath();
        String[] paths = columnPath.split("\\.");
        Class<?> fieldClassType = null;
        for( String path : paths ){

            if( fieldClassType == null ) {
                fieldClassType = columnData.getColumnTableData().getTypeDesc().getObjectClass();
            }
            //for example: for first loop iteration at this point fieldClassType has value: class teammember.TeamMemberInfo
            //path has value "teamMemberKey"

            fieldClassType = fieldClassType.getDeclaredField( path ).getType();
            //for example: for first loop iteration at this point fieldClassType has value: class class teammember.TeamMemberKey
        }

        return fieldClassType;
    }

    /**
     * @throws SQLException
     */
    private void validateAndPrepareSelectColumns() throws SQLException {
        //then we check the selected columns
        for (int i = 0; i < getQueryColumns().size(); i++) {
            SelectColumn sc = getQueryColumns().get(i);

            sc.createColumnData(this);


            //now replace all columns where needed
            if (sc.isAllColumns() && !sc.isFunction()) {

                isSelectAll = true;
                getQueryColumns().remove(i);
                if (sc.getColumnTableData() == null) {
                    //get columns from all tables

                    for (int t = 0; t < _tablesData.size(); t++) {
                        QueryTableData queryTableData = _tablesData.get(t);
                        List<SelectColumn> toAdd = getWildcardColumns(queryTableData);
                        getQueryColumns().addAll(i, toAdd);
                        i += toAdd.size();
                    }


                } else {
                    List<SelectColumn> toAdd = getWildcardColumns(sc.getColumnTableData());
                    getQueryColumns().addAll(i, toAdd);
                    i += toAdd.size();

                }
                i--;

            }

        }

        addAbsentColumns();
    }

    /**
     * Add all columns of given table
     */
    private List<SelectColumn> getWildcardColumns(QueryTableData queryTableData) throws SQLException {
        ITypeDesc info = queryTableData.getTypeDesc();
        if (info == null)
            return Collections.emptyList();
        List<SelectColumn> toAdd = new ArrayList<SelectColumn>(info.getNumOfFixedProperties());
        for (int i = 0; i < info.getNumOfFixedProperties(); i++) {
            SelectColumn newColumn = new SelectColumn(queryTableData, info.getFixedProperty(i).getName());
            toAdd.add(newColumn);

        }
        return toAdd;
    }

    /**
     * @throws SQLException
     *
     */
    private void addAbsentColumns() throws SQLException {
        ITypeDesc info;
        if (isAddAbsentCol) {
            QueryTableData tableData = getTableData();
            if(tableData.getTypeDesc() != null) {
                info = tableData.getTypeDesc();
                for (int c = 0; c < info.getNumOfFixedProperties(); c++) {
                    boolean found = false;
                    Iterator<SelectColumn> iter = getQueryColumns().iterator();
                    while (iter.hasNext()) {
                        SelectColumn col = (SelectColumn) iter.next();

                        if (col.getColumnData().getColumnName() != null && col.getColumnData().getColumnName().equals(info.getFixedProperty(c).getName())) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        SelectColumn newColumn = new SelectColumn(tableData, info.getFixedProperty(c).getName());

                        newColumn.setVisible(false);
                        getQueryColumns().add(newColumn);
                    }
                }
            }
            else if(tableData.getSubQuery() != null){
                //TODO consider treating this
            }
            else
                throw  new IllegalStateException("NO table name an d no sub query");
        }
    }

    /**
     */
    private void validateNotifyQuery() throws SQLException {
        if (isJoined())
            throw new SQLException("Operation doesn't support multiple tables.");

        if (isGroupBy())
            throw new SQLException("Operation doesn't support 'group by'.");

        if (isJoined())
            throw new SQLException("Operation doesn't support join queries.");

        if (isOrderBy())
            throw new SQLException("Operation doesn't support 'order by'.");

        if (isAggFunction())
            throw new SQLException("Operation doesn't support aggregation.");

        if (isDistinct())
            throw new SQLException("Operation doesn't support 'distinct'.");

        if (forUpdate)
            throw new SQLException("Operation doesn't support 'for update'.");

    }

    private boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    /**
     * Executes a select query without a where clause from one table
     */
    private IQueryResultSet<IEntryPacket> executeEmptyQuery(ISpaceProxy space, Transaction txn, IQueryResultSet<IEntryPacket> entries) throws Exception {
        // no where clause and no join. read everything
        int size = getEntriesLimit();

        QueryTableData tableData = getTableData();
        if(tableData.getSubQuery() != null){
            return tableData.executeSubQuery(space, txn, true);
        }
        QueryTemplatePacket template = new QueryTemplatePacket(tableData, _queryResultType);

        //  Handle notify queries
        if (isBuildOnly()) {
            entries = new ArrayListResult();
            entries.add(template);

            return entries;
        }

        if (useAggregationApi(txn))
            template.setAggregationSet(_aggregationSet);
        return template.read(space, this, txn, getReadModifier(), size);

    }

    /**
     * Aggregation api can be disabled by setting com.gigaspaces.query.useAggregationsApi=false
     */
    private boolean useAggregationApi(Transaction txn) {

        if (!useAggregationsApi)
            return false;

        if (txn != null)
            return false;

        if (isJoined())
            return false;

        if (getGroupColumn() != null)
            return true;
        else if (getOrderColumns() != null && !isAggFunction())
            return true;
        else if (isAggFunction())
            return true;

        return false;
    }

    /**
     * Executes a select query without a where clause but from several tables
     */
    private IQueryResultSet<IEntryPacket> executeJoinedQuery(ISpaceProxy space, Transaction txn) throws RemoteException, TransactionException,
            UnusableEntryException, SQLException {
        return _executor.execute(space, txn, getReadModifier(), getEntriesLimit());
    }

    /**
     * Creates the projection indices for all visible query columns
     */
    private void createProjectionIndices(IQueryResultSet<IEntryPacket> entries) {

        if (isJoined()) {
            for (IEntryPacket entry : entries) {
                JoinedEntry joinedEntry = (JoinedEntry) entry;

                joinedEntry.createProjection(getQueryColumns());
            }
        }

        int projIndex = 0;
        boolean isProjected = isGroupBy() || isAggFunction() || isJoined();

        for (SelectColumn col : getQueryColumns()) {

            if (col.isVisible()) {
                if (isProjected)
                    col.setProjectedIndex(projIndex++);
                else
                    col.setProjectedIndex(col.getColumnIndexInTable());
            }

        }

        if (isOrderBy()) {
            if (_projectionTemplate == null) {
                projIndex = 0;
                for (SelectColumn col : getOrderColumns()) {
                    if (isProjected)
                        col.setProjectedIndex(projIndex++);
                    else
                        col.setProjectedIndex(col.getColumnIndexInTable());

                }
            }

        }

    }

    private void createProjectionTemplate() {

        if (_projectionTemplate != null || !isConvertResultToArray() || isSelectAll)
            return;

        ArrayList<String> projectedProperties = new ArrayList<String>(getQueryColumns().size());
        for (SelectColumn col : getQueryColumns()) {
            if (col.isVisible() && !col.isAllColumns()) {
                projectedProperties.add(col.getName());
            }
        }

        if (!projectedProperties.isEmpty()) {
            if (isOrderBy()) {
                for (SelectColumn col : getOrderColumns()) {
                    col.setProjectedIndex(projectedProperties.indexOf(col.getName()));
                }
            }

            _projectionTemplate = ProjectionTemplate.create(projectedProperties.toArray(new String[projectedProperties.size()]), getTypeInfo());
        }
    }

    public ArrayList<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    @Override
    public boolean isSelectQuery() {
        return true;
    }

    @Override
    public BatchResponsePacket executePreparedValuesBatch(ISpaceProxy space,
                                                          Transaction transaction, PreparedValuesCollection preparedValuesCollection)
            throws SQLException {
        throw new SQLException("Batching is not supported for SELECT queries.");
    }

    /**
     * Adds the dynamic columns of specific query to the query columns so they will be shown in the
     * result set
     */
    private void addDynamicSelectColumns(IQueryResultSet<IEntryPacket> entries)
            throws SQLException {
        HashMap<String, QueryTableData> dynamicPropertiesTables = new HashMap<String, QueryTableData>();
        HashMap<QueryTableData, HashMap<String, SelectColumn>> dynamicColumnsMap = new HashMap<QueryTableData, HashMap<String, SelectColumn>>();

        // find dynamic tables
        for (QueryTableData tableData : getTablesData()) {

            if (tableData.hasAsterixSelectColumns()
                    && tableData.getTypeDesc().supportsDynamicProperties()) {
                dynamicPropertiesTables
                        .put(tableData.getTableName(), tableData);
                dynamicColumnsMap.put(tableData,
                        new HashMap<String, SelectColumn>());
            }
        }

        if (!dynamicPropertiesTables.isEmpty()) {

            //add all the dynamic properties to the result set
            for (IEntryPacket entryPacket : entries) {
                if (isJoined()) {
                    JoinedEntry joinedEntry = (JoinedEntry) entryPacket;
                    for (int i = 0; i < joinedEntry.getSize(); i++) {

                        IEntryPacket entry = joinedEntry.getEntry(i);
                        QueryTableData table = getTablesData().get(i);
                        addDynamicColumns(table, dynamicColumnsMap, entry);
                    }
                } else {
                    QueryTableData table = getTableData();
                    addDynamicColumns(table, dynamicColumnsMap, entryPacket);
                }
            }

            for (HashMap<String, SelectColumn> dynamicColumns : dynamicColumnsMap
                    .values()) {
                for (SelectColumn dynamicColumn : dynamicColumns.values()) {
                    getQueryColumns().add(dynamicColumn);
                }
            }
        }
    }

    private void addDynamicColumns(
            QueryTableData table,
            HashMap<QueryTableData, HashMap<String, SelectColumn>> dynamicColumnsMap,
            IEntryPacket entryPacket) throws SQLException {
        Map<String, Object> dynamicProperties = entryPacket.getDynamicProperties();
        if (dynamicProperties == null || dynamicProperties.size() == 0)
            return;

        Set<String> dynamicPropertiesNames = dynamicProperties.keySet();
        HashMap<String, SelectColumn> dynamicColumns = dynamicColumnsMap.get(table);
        for (String prop : dynamicPropertiesNames) {
            //add column if doesn't exist
            if (!dynamicColumns.containsKey(prop)) {
                SelectColumn dynamicColumn = new SelectColumn(table, prop, true);
                dynamicColumns.put(dynamicColumn.getName(), dynamicColumn);
            }
        }
    }

    public boolean isFlattenResults() {
        return flattenResults;
    }

    public void setFlattenResults(boolean flattenResults) {
        this.flattenResults = flattenResults;
    }
}
