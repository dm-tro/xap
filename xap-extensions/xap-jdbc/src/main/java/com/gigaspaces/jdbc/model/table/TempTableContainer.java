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
package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.List;
import java.util.stream.Collectors;

public class TempTableContainer extends TableContainer {
    private final QueryResult tableResult;
    private final String alias;
    private TableContainer joinedTable;

    public TempTableContainer(QueryResult tableResult, String alias) {
        this.tableResult = tableResult;
        this.alias = alias;
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) {
        return tableResult;
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias, boolean visible) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public List<QueryColumn> getVisibleColumns() {
        return tableResult.getQueryColumns();
    }

    @Override
    public List<String> getAllColumnNames() {
        return tableResult.getQueryColumns().stream().map(QueryColumn::getName).collect(Collectors.toList());
    }

    @Override
    public String getTableNameOrAlias() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public TableContainer getJoinedTable() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoinedTable(TableContainer joinedTable) {
        this.joinedTable = joinedTable;
    }

    @Override
    public QueryResult getQueryResult() {
        return tableResult;
    }

    @Override
    public void setLimit(Integer value) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public QueryTemplatePacket createQueryTemplatePacketWithRange(Range range) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setQueryTemplatePackage(QueryTemplatePacket queryTemplatePacket) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoined(boolean joined) {

    }

    @Override
    public boolean isJoined() {
        return false;
    }

    @Override
    public Object getColumnValue(String columnName, Object value) {
        throw new UnsupportedOperationException("Not supported yet!");
    }
}