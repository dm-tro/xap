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

package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.internal.utils.Singletons;
import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricTagsSnapshot;
import com.j_spaces.kernel.SystemProperties;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.gigaspaces.metrics.hsqldb.HsqlDBMetricsUtils.*;

/**
 * @author Evgeny
 * @since 15.0
 */
public class HsqlDbReporter extends MetricReporter {

    private static final Logger _logger = Logger.getLogger( HsqlDbReporter.class.getName() );
    private Connection con = null;
    private Map<String,PreparedStatement> _preparedStatements = new HashMap<>();
    private final static String hsqldDbConnectionKey = "HSQL_DB_CONNECTION";

    private static final Object _lock = new Object();

    private final Set<String> recordedMetricsTablesSet =
        new HashSet<>( Arrays.asList( METRICS_TABLES ) );

    //by default false
    private final boolean isAllMetricsRecordedToHsqlDb =
                        Boolean.getBoolean( SystemProperties.RECORDING_OF_ALL_METRICS_TO_HSQLDB_ENABLED);


    public HsqlDbReporter(HsqlDBReporterFactory factory) {
        super(factory);

        String driverClassName = factory.getDriverClassName();
        String password = factory.getPassword();
        String username = factory.getUsername();
        String url = factory.getConnectionUrl();
        try {
            new Thread(() -> con = createConnection(driverClassName, url, username, password))
                .start();
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, e.toString(), e);
            }
        }
    }

    private Connection createConnection( String driverClassName, String url, String username, String password ) {
        Connection con = null;
        try {
            _logger.fine("Loading " + driverClassName);
            Class.forName(driverClassName);
            while (con == null) {
                try {
                    synchronized (_lock) {
                        if ((con = (Connection) Singletons.get(hsqldDbConnectionKey)) == null) {
                            _logger.fine("Connecting to " + url);
                            con = DriverManager.getConnection(url, username, password);
                            Singletons.putIfAbsent(hsqldDbConnectionKey, con);
                            _logger.info("Connection to [" + url + "] successfully created");
                            retrieveExistingTablesInfo( con );
                        }
                    }
                }
                catch( Exception e ){
                    if( _logger.isLoggable( Level.WARNING ) ){
                        _logger.log( Level.WARNING, e.toString() );
                    }
                }
                if( con == null ){
                    try {
                        Thread.sleep( 1 * 1000 );
                    } catch (InterruptedException e) {

                    }
                }
            }
        }
        catch (Exception e) {
            if( _logger.isLoggable( Level.SEVERE ) ){
                _logger.log( Level.SEVERE, e.toString(), e );
            }
        }

        return con;
    }

    private void retrieveExistingTablesInfo( Connection con ) throws SQLException {

        if( _logger.isLoggable( Level.FINER ) ){
            _logger.finer("! Existing public tables are:");

            DatabaseMetaData mtdt = con.getMetaData();
            String catalog = con.getCatalog();
            ResultSet rs = mtdt.getTables(catalog, "PUBLIC", "%", null);
            try {
                //ResultSetMetaData rsmd = rs.getMetaData();
                StringBuilder strBuilder = new StringBuilder("\n");
                while (rs.next()) {
                    strBuilder.append(rs.getString( "TABLE_SCHEM" ));
                    strBuilder.append(".");
                    strBuilder.append(rs.getString( "TABLE_NAME" ));
                    strBuilder.append("\n");
                }
                _logger.fine(strBuilder.toString());
            }
            finally{
                rs.close();
            }
        }
    }

    public void report(List<MetricRegistrySnapshot> snapshots) {
        super.report(snapshots);
        //flush();
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, String key, Object value) {
        // Save length before append:
       //String row1 = "insert into " + realDbTableName + " values('23','AABBAABB','Address','NY','AB',23500)";

        if( _logger.isLoggable( Level.FINEST ) ) {
            _logger.finest("Report, con=" + con + ", key=" + key );
        }

        if( con == null ){
            return;
        }

        if( isAllMetricsRecordedToHsqlDb || recordedMetricsTablesSet.contains( key ) ) {

            final String realDbTableName = createValidTableName(key );

            Set<Map.Entry<String, Object>> tagEntries = tags.getTags().entrySet();
            StringBuilder columns = new StringBuilder();
            StringBuilder values = new StringBuilder();
            Map<Integer, Object> insertRowQueryValues = new HashMap<>();

            columns.append("TIME");
            columns.append(',');

            values.append('?');
            values.append(',');

            insertRowQueryValues.put(1, new Timestamp(snapshot.getTimestamp()));

            int index = 2;//it's 2 since TIME as a first parameter just added
            for (Map.Entry<String, Object> entry : tagEntries) {
                String entryKey = entry.getKey();
                Object entryValue = entry.getValue();

                if (_logger.isLoggable(Level.FINEST)) {
                    _logger.finest(
                        ">>> KEY=" + entryKey + ", value class name:" + entryValue.getClass()
                            .getName()
                        + ", value=" + entryValue);
                }

                columns.append(entryKey);
                columns.append(',');

                values.append('?');//transformValue( entryValue ) );
                values.append(',');

                insertRowQueryValues.put(index, entryValue);

                index++;
            }

            columns.append("VALUE");
            values.append('?');
            insertRowQueryValues.put(index, value);

            final String
                insertSQL =
                "INSERT INTO " + realDbTableName + " (" + columns + ") VALUES (" + values + ")";

            try {
                PreparedStatement insertQueryPreparedStatement = _preparedStatements.get(insertSQL);
                if (_logger.isLoggable(Level.FINER)) {
                    _logger.finer(
                        "insert row query before setting values [" + insertSQL + "], statement:"
                        + insertQueryPreparedStatement);
                }

                if (insertQueryPreparedStatement == null) {
                    insertQueryPreparedStatement = con.prepareStatement(insertSQL);
                    //cache just created PreparedStatement if was not stored before
                    _preparedStatements.put(insertSQL, insertQueryPreparedStatement);
                }

                Set<Map.Entry<Integer, Object>>
                    insertRowQueryEntries =
                    insertRowQueryValues.entrySet();
                for (Map.Entry<Integer, Object> entry : insertRowQueryEntries) {
                    Integer paramIndex = entry.getKey();
                    Object paramValue = entry.getValue();
                    populatePreparedStatementWithParameters(insertQueryPreparedStatement,
                                                            paramIndex, paramValue);
                }
                if (_logger.isLoggable(Level.FINER)) {
                    _logger.finer(">>> Before insert [" + insertSQL + "]");
                }

                insertQueryPreparedStatement.executeUpdate();

                if (_logger.isLoggable(Level.FINER)) {
                    _logger.finer(">>> After insert [" + insertSQL + "]");
                }
            } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
                String message = sqlSyntaxErrorException.getMessage();
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine(
                        ">>>@@@ exception message=" + message + ", realDbTableName="
                        + realDbTableName);
                }
                //if such table does not exist
                if (message != null && message.contains(
                    "user lacks privilege or object not found: " + realDbTableName)) {
                    //create such (not found ) table
                    String tableColumnsInfo = createTableColumnsInfo(tags, value);

                    try {
                        createTable(con, realDbTableName, tableColumnsInfo);
                    } catch (SQLException e) {
                        if (_logger.isLoggable(Level.WARNING)) {
                            _logger.log(Level.WARNING, e.toString(), e);
                        }
                        //probably create table failed since table was just created, then try to
                        //call to this report method again, TODO: prevent loop
                        //report( snapshot, tags, key, value);
                    }
                }
                //any column does not exist
                else if (message != null &&
                         message.contains("user lacks privilege or object not found: ")) {

                    try {
                        handleAddingMissingTableColumns(con, tags, value, realDbTableName);
                    } catch (SQLException e) {
                        if (_logger.isLoggable(Level.SEVERE)) {
                            _logger.log(Level.SEVERE,
                                        "Failed to add missing columns to table [" +
                                        realDbTableName + "] due to:" + e.toString(), e);
                        }
                    }
                }
            } catch (SQLException e) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE,
                                "Exception thrown while inserting row [" + insertSQL + "] , " + e
                                    .toString(), e);
                }
            }
        }
    }

    private void handleAddingMissingTableColumns( Connection con, MetricTagsSnapshot tags, Object value, String realDbTableName )
        throws SQLException {

            DatabaseMetaData dbm = con.getMetaData();
            ResultSet columnsResultSet = dbm.getColumns(null, null, realDbTableName, null);
            if( _logger.isLoggable( Level.FINER ) ) {
                _logger.finer("~~~ Existing table, name:" + realDbTableName + ", its columns:");
            }
            List<String> existingTableColumns = new ArrayList<>();
            try {
                while( columnsResultSet.next() ){
                    String columnName = null;
                    try {
                        columnName = columnsResultSet.getString("COLUMN_NAME");

                        if( _logger.isLoggable( Level.FINER ) ) {
                            _logger.finer("col name=" + columnName +
                                         ", col type=" + columnsResultSet.getString("TYPE_NAME"));
                        }
                        existingTableColumns.add( columnName.toUpperCase() );
                    } catch (SQLException e) {
                        if( _logger.isLoggable( Level.SEVERE ) ){
                            _logger.log( Level.SEVERE, e.toString(), e );
                        }
                    }
                }
            } catch (SQLException e) {
                if( _logger.isLoggable( Level.SEVERE ) ){
                    _logger.log( Level.SEVERE, e.toString(), e );
                }
            }

            Map<String, String> newTableColumnsMap = createTableColumnsMap(tags, value);
            Set<Map.Entry<String, String>> newTableColumnsEntries = newTableColumnsMap.entrySet();

            if( _logger.isLoggable( Level.FINER ) ) {
                _logger.finer("~~~ passed for create table parameters:" + Arrays
                    .toString(newTableColumnsEntries
                    .toArray(new Map.Entry[newTableColumnsEntries.size()])));
            }

            //String prevColumnName = null;
            for( Map.Entry<String, String> newTableColumnEntry : newTableColumnsEntries ){
                String columnName = newTableColumnEntry.getKey().toUpperCase();
                if( _logger.isLoggable( Level.FINEST ) ) {
                    //check if already exists in existing table
                    _logger.finest("> CONTAINS of " + columnName + " in list " +
                                   Arrays.toString(existingTableColumns.toArray(
                                       new String[existingTableColumns.size()])) +
                                   ":" + existingTableColumns.contains(columnName));
                }

                if( !existingTableColumns.contains( columnName ) ){
                    String columnType = newTableColumnEntry.getValue();
                    if( _logger.isLoggable( Level.FINER ) ) {
                        _logger.finer("Before adding new column [" + columnName + " " + columnType
                                     + "] to table " + realDbTableName);
                    }
                    //String columnLocation = prevColumnName == null ? " FIRST " : " AFTER " + prevColumnName;
                    String addColumnQuery = "ALTER TABLE " + realDbTableName + " ADD " + columnName + " " + columnType; /*+ columnLocation*/
                    if( _logger.isLoggable( Level.FINER ) ) {
                        _logger.finer("> addColumnQuery=" + addColumnQuery );
                    }
                    Statement statement = null;
                    try {
                        statement = con.createStatement();
                        statement.executeUpdate(addColumnQuery);
                        if (_logger.isLoggable(Level.FINER)) {
                            _logger.finer(
                                "ADDED new column [" + columnName + " " + columnType + "] to table "
                                + realDbTableName);
                        }
                        //createIndexOnTableAfterAddingColumn( statement, realDbTableName, columnName );
                    }
                    catch( SQLSyntaxErrorException sqlExc){
                        String exceptionMessage = sqlExc.getMessage();
                        //since sometimes at teh same times can be fet attempts to add the same column to the same table
                        if (exceptionMessage == null ||
                            !exceptionMessage.contains("object name already exists in statement" ) ) {
                            if( _logger.isLoggable( Level.SEVERE ) ){
                                _logger.log( Level.SEVERE, sqlExc.toString(), sqlExc );
                            }
                        }
                    }
                    catch (SQLException e1) {
                        if( _logger.isLoggable( Level.SEVERE ) ){
                            _logger.log( Level.SEVERE, e1.toString(), e1 );
                        }
                    }
                    finally{
                        if( statement != null ){
                            try {
                                statement.close();
                            }
                            catch (SQLException e){
                                if( _logger.isLoggable( Level.WARNING ) ){
                                    _logger.log( Level.WARNING, e.toString(), e );
                                }
                            }
                        }
                    }
                }
            }
    }

    private String createTableColumnsInfo( MetricTagsSnapshot tags, Object value) {

        Set<Map.Entry<String, Object>> entries = tags.getTags().entrySet();

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append( '(' );
        strBuilder.append( "TIME" );
        strBuilder.append( ' ' );
        strBuilder.append( "TIMESTAMP" );
        strBuilder.append( ',' );

        for( Map.Entry<String, Object> entry : entries ){
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();

            strBuilder.append( columnName );
            strBuilder.append( ' ' );
            strBuilder.append( getHSQLDBDataType( columnName, columnValue ) );
            strBuilder.append( ',' );
        }

        strBuilder.append( "VALUE" );
        strBuilder.append( ' ' );
        strBuilder.append( getHSQLDBDataType( "VALUE", value ) );
        strBuilder.append( ')' );

        if( _logger.isLoggable( Level.FINER ) ) {
            _logger.finer("tables columns info=" +strBuilder);
        }

        return strBuilder.toString();
    }

    //return map where key is column name, value is type
    private Map<String,String> createTableColumnsMap( MetricTagsSnapshot tags, Object value) {

        Set<Map.Entry<String, Object>> entries = tags.getTags().entrySet();
        //save insertion order
        Map<String,String> retColumnsMap = new LinkedHashMap<>(entries.size() );

        retColumnsMap.put( "TIME", "TIMESTAMP" );

        for( Map.Entry<String, Object> entry : entries ){
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            retColumnsMap.put( columnName, getHSQLDBDataType( columnName, columnValue ) );
        }

        retColumnsMap.put( "VALUE", getHSQLDBDataType( "VALUE", value ) );

        if( _logger.isLoggable( Level.FINER ) ) {
            _logger.finer("retColumnsMap=" + Arrays
                .toString(retColumnsMap.entrySet().toArray(new Map.Entry[retColumnsMap.size()])));
        }

        return retColumnsMap;
    }

    private void populatePreparedStatementWithParameters(
        PreparedStatement insertQueryPreparedStatement, Integer paramIndex, Object paramValue) throws SQLException{

        if( paramValue instanceof String ) {
            insertQueryPreparedStatement.setString( paramIndex, paramValue.toString() );
        }
        else if( paramValue instanceof Timestamp ){
            insertQueryPreparedStatement.setTimestamp( paramIndex, ( Timestamp )paramValue );
        }
        else if( paramValue instanceof Integer ){
            insertQueryPreparedStatement.setInt( paramIndex, ( Integer )paramValue );
        }
        else if( paramValue instanceof Long ){
            insertQueryPreparedStatement.setLong( paramIndex, ( Long )paramValue );
        }
        else if( paramValue instanceof Double ){
            insertQueryPreparedStatement.setDouble( paramIndex, ( Double )paramValue );
        }
        else if( paramValue instanceof Float ){
            insertQueryPreparedStatement.setDouble( paramIndex, ( ( Float )paramValue ).doubleValue() ); ;
        }
        else if( paramValue instanceof Boolean ){
            insertQueryPreparedStatement.setBoolean( paramIndex, ( Boolean )paramValue );
        }
        else{
            _logger.warning( "@@@ Value [" + paramValue + "] of class [" +
                             paramValue.getClass().getName() + "] "+
                             "with index [" + paramIndex + "] was not set");
        }
    }

    private String getHSQLDBDataType( String name, Object value ){

        String lowerCaseName = name.toLowerCase();
        if( value instanceof String ){
            String type;
            switch ( lowerCaseName){
                case "pid":
                    type = "VARCHAR(10)";
                    break;

                case "process_name":
                    type = "VARCHAR(10)";
                    break;

                case "ip":
                    type = "VARCHAR(15)";
                    break;

                case "pu_instance_id":
                    type = "VARCHAR(10)";
                    break;

                case "space_instance_id":
                    type = "VARCHAR(8)";
                    break;
                //in the case of ec2 instance host name can be long, like:
                //ip-xxx-xxx-xxx-xxx.eu-west-1.compute.internal
                case "host":
                    type = "VARCHAR(80)";
                    break;

                default:
                    type = "VARCHAR(40)";
            }

            return type;
        }
        if( value instanceof Timestamp ){
            return "TIMESTAMP";
        }
        if( value instanceof Boolean ){
            return "BOOLEAN";
        }
        if( value instanceof Number ){
            if( value instanceof Long ){
                return "BIGINT";
            }
            if( value instanceof Integer ){
                return "INTEGER";
            }
            if( value instanceof Short ){
                return "SMALLINT";
            }
            if( value instanceof Double ){
                return "REAL";
            }
            if( value instanceof Float ){
                return "REAL";
            }

            return "NUMERIC";
        }

        return "VARCHAR(40)";
    }

    @Override
    public void close() {
        super.close();
        if( con != null ) {
            try {
                con.close();
            } catch (SQLException e) {
                if( _logger.isLoggable( Level.WARNING ) ){
                    _logger.log( Level.WARNING, e.toString(), e );
                }
            }
        }
    }

    private void createTable( Connection con, String tableName, String columnsInfo ) throws SQLException {
        Statement statement = null;
        try {
            statement = con.createStatement();

            final String sql = "CREATE CACHED TABLE " + tableName + " " + columnsInfo;

            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Create [" + tableName + "] with sql query [" + sql + "]");
            }
            statement.executeUpdate(sql);
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Table [" + tableName + "] successfully created");
            }
            createIndexOnTable( statement, tableName );
        }
        finally{
            if( statement != null ){
                try {
                    statement.close();
                }
                catch( SQLException e ){
                    if( _logger.isLoggable( Level.WARNING ) ){
                        _logger.log( Level.WARNING, e.toString(), e );
                    }
                }
            }
        }
    }

    private void createIndexOnTable(Statement statement, String tableName) throws SQLException {
        String sql = "CREATE INDEX gsindex_" + tableName + " ON " + tableName + " ( TIME ASC )";//SPACE_ACTIVE
/*
        if( tableName.equals( TABLE_NAME_JVM_MEMORY_HEAP_USED_BYTES ) ||
            tableName.equals( TABLE_NAME_JVM_MEMORY_HEAP_USED_PERCENT ) ||
            tableName.equals(TABLE_NAME_PROCESS_CPU_USED_PERCENT ) ||
            tableName.equals( TABLE_NAME_SPACE_REPLICATION_REDO_LOG_USED_PERCENT ) ){
            sql = "CREATE INDEX gsindex_" + System.currentTimeMillis() + " ON " + tableName + " ( TIME ASC )";//PU_INSTANCE_ID
        }
        //space operations throughput
        else if( tableName.equals( TABLE_NAME_SPACE_OPERATIONS_EXECUTE_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_READ_MULTIPLE_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_READ_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_TAKE_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_TAKE_MULTIPLE_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_WRITE_TP ) ){
            sql = "CREATE INDEX gsindex_" + System.currentTimeMillis() + " ON " + tableName + " ( TIME ASC )";//SPACE_ACTIVE
        }*/
//        if( sql != null ){
        if( _logger.isLoggable( Level.FINE ) ) {
            _logger.fine(
                "Creating index for table [" + tableName + "] by executing [" + sql + "]");
        }
        statement.executeUpdate(sql);
        if( _logger.isLoggable( Level.FINE ) ) {
            _logger.fine("Index successfully created");
        }
/*
        }
        else{
            _logger.info( "Index was not created for table [" + tableName + "]" );
        }
*/
    }
/*
    private void createIndexOnTableAfterAddingColumn(Statement statement, String tableName, String columnName ) throws SQLException {
        String sql = null;

        if( ( tableName.equals( TABLE_NAME_JVM_MEMORY_HEAP_USED_BYTES ) ||
            tableName.equals( TABLE_NAME_JVM_MEMORY_HEAP_USED_PERCENT ) ||
            tableName.equals(TABLE_NAME_PROCESS_CPU_USED_PERCENT ) ) &&
            columnName.equals( "PU_INSTANCE_ID" ) ){
            sql = "CREATE INDEX gsindex_" + System.currentTimeMillis() + " ON " + tableName + " ( PU_INSTANCE_ID )";//PU_INSTANCE_ID
        }
        if( sql != null ){
            _logger.info( "Creating index for table [" + tableName + "] by executing [" + sql + "]" );
            statement.executeUpdate(sql);
            _logger.info( "Index successfully created" );
        }
        else{
            _logger.info( "Index was not created for table [" + tableName + "]" );
        }
    }*/
}