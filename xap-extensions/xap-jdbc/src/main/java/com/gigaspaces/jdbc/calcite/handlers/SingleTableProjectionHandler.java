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
package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.model.table.*;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCastFunction;

import java.util.ArrayList;
import java.util.List;

public class SingleTableProjectionHandler extends RexShuttle {
    private final RexProgram program;
    private final TableContainer tableContainer;
    private final List<String> inputFields;
    private final List<String> outputFields;
    private final boolean isRoot;
    private final QueryExecutor queryExecutor;
    private final LocalSession session;

    public SingleTableProjectionHandler(LocalSession session, RexProgram program, TableContainer tableContainer, boolean isRoot, QueryExecutor queryExecutor) {
        this.program = program;
        this.tableContainer = tableContainer;
        this.inputFields = program.getInputRowType().getFieldNames();
        this.outputFields = program.getOutputRowType().getFieldNames();
        this.isRoot = isRoot;
        this.queryExecutor = queryExecutor;
        this.session = session;
    }

    public void project(){
        List<RexLocalRef> projects = program.getProjectList();
        for (int i = 0; i < projects.size(); i++) {
            RexLocalRef localRef = projects.get(i);
            RexNode node = program.getExprList().get(localRef.getIndex());
            if(node.isA(SqlKind.INPUT_REF)){
                RexInputRef inputRef = (RexInputRef) node;
                String alias = outputFields.get(i);
                String originalName = inputFields.get(inputRef.getIndex());
                if(!originalName.startsWith("EXPR"))
                    tableContainer.addQueryColumn(originalName, alias, true, isRoot ? i : 0);
            }
            else if(node instanceof RexCall){
                RexCall call = (RexCall) node;
                SqlFunction sqlFunction;
                List<IQueryColumn> queryColumns = new ArrayList<>();
                IQueryColumn functionCallColumn = null;
                switch (call.getKind()) {
                    case EXTRACT:
                    case OTHER_FUNCTION:
                        sqlFunction = (SqlFunction) call.op;
                        addQueryColumns(call, queryColumns);
                        functionCallColumn = new FunctionCallColumn(session, queryColumns, sqlFunction.getName(), sqlFunction.toString(), null, isRoot, -1);
                        if(isRoot)
                            tableContainer.getVisibleColumns().add(functionCallColumn);
                        else
                            tableContainer.getInvisibleColumns().add(functionCallColumn);
                        break;
                    case CAST:
                        sqlFunction = (SqlCastFunction) call.op;
                        addQueryColumns(call, queryColumns);
                        functionCallColumn = new FunctionCallColumn(session, queryColumns, sqlFunction.getName(), sqlFunction.toString(), null, isRoot, -1, call.getType().getFullTypeString());
                        if(isRoot)
                            tableContainer.getVisibleColumns().add(functionCallColumn);
                        else
                            tableContainer.getInvisibleColumns().add(functionCallColumn);
                        break;
                    case CASE:
                        CaseColumn caseColumn = new CaseColumn(outputFields.get(i), CalciteUtils.getJavaType(call), i);
                        addCaseCondition(call, caseColumn);
                        queryExecutor.addCaseColumn(caseColumn);
                        break;
                    default:
                        throw new UnsupportedOperationException("call of kind " + call.getKind() + " is not supported");

                }
            }
            else if(node.isA(SqlKind.LITERAL)){
                RexLiteral literal = (RexLiteral) node;
                int columnIndex = i; //from projection loop
                LiteralColumn literalColumn = new LiteralColumn(CalciteUtils.getValue(literal), columnIndex);
                if(isRoot) {
                    tableContainer.getVisibleColumns().add(literalColumn);
                } else {
                    tableContainer.getInvisibleColumns().add(literalColumn);
                }
            }
        }
    }

    private void addQueryColumns(RexCall call, List<IQueryColumn> queryColumns) {
        for (RexNode operand : call.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = program.getExprList().get(((RexLocalRef) operand).getIndex());
                if (rexNode.isA(SqlKind.INPUT_REF)) {
                    RexInputRef rexInputRef = (RexInputRef) rexNode;
                    String column = inputFields.get(rexInputRef.getIndex());
                    queryColumns.add(tableContainer.addQueryColumnWithoutOrdinal(column, null, false));
                }
                else if (rexNode.isA(SqlKind.LITERAL)) {
                    RexLiteral literal = (RexLiteral) rexNode;
                    int columnOrdinal = ((RexLocalRef) operand).getIndex();
                    queryColumns.add(new LiteralColumn(CalciteUtils.getValue(literal), columnOrdinal));
                }
            }
        }
    }

    private void addCaseCondition(RexCall call, CaseColumn caseColumn) {
        CaseConditionHandler caseHandler = new CaseConditionHandler(program, queryExecutor, inputFields,
                tableContainer, caseColumn);
        caseHandler.visitCall(call);
    }
}
