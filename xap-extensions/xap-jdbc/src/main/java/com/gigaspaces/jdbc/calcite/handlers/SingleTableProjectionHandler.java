package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.model.table.*;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

import static com.gigaspaces.jdbc.model.table.IQueryColumn.EMPTY_ORDINAL;

public class SingleTableProjectionHandler extends RexShuttle {
    private final RexProgram program;
    private final TableContainer tableContainer;
    private final List<String> inputFields;
    private final List<String> outputFields;
    private final QueryExecutor queryExecutor;
    private final LocalSession session;

    public SingleTableProjectionHandler(LocalSession session, RexProgram program, TableContainer tableContainer, QueryExecutor queryExecutor) {
        this.program = program;
        this.tableContainer = tableContainer;
        this.inputFields = program.getInputRowType().getFieldNames();
        this.outputFields = program.getOutputRowType().getFieldNames();
        this.queryExecutor = queryExecutor;
        this.session = session;
    }

    public void project(){
        List<RexLocalRef> projects = program.getProjectList();
        for (int i = 0; i < projects.size(); i++) {
            String alias = outputFields.get(i);
            RexLocalRef localRef = projects.get(i);
            RexNode node = program.getExprList().get(localRef.getIndex());
            if(node.isA(SqlKind.INPUT_REF)){
                RexInputRef inputRef = (RexInputRef) node;
                String originalName = inputFields.get(inputRef.getIndex());
                if(!originalName.startsWith("EXPR")) {
                    IQueryColumn qc = tableContainer.addQueryColumnWithoutOrdinal(originalName, alias, true);
                    tableContainer.addProjectedColumn(qc);
                }
            }
            else if(node instanceof RexCall){
                RexCall call = (RexCall) node;
                SqlFunction sqlFunction;
                List<IQueryColumn> queryColumns = new ArrayList<>();
                IQueryColumn functionCallColumn;
                switch (call.getKind()) {
                    case EXTRACT:
                    case OTHER_FUNCTION:
                    case CAST:
                        sqlFunction = (SqlFunction) call.op;
                        addQueryColumns(call, queryColumns, inputFields, outputFields, i);
                        functionCallColumn = new FunctionColumn(session, queryColumns, sqlFunction.getName(),
                                sqlFunction.toString(), alias, true, EMPTY_ORDINAL,call.getType().getFullTypeString());
                        tableContainer.getVisibleColumns().add(functionCallColumn);
                        tableContainer.addProjectedColumn(functionCallColumn);
                        break;
                    case CASE:
                        CaseColumn caseColumn = new CaseColumn(outputFields.get(i), CalciteUtils.getJavaType(call), EMPTY_ORDINAL);
                        addCaseCondition(call, caseColumn);
                        tableContainer.addProjectedColumn(caseColumn);
                        break;
                    default:
                        throw new UnsupportedOperationException("call of kind " + call.getKind() + " is not supported");

                }
            }
            else if(node.isA(SqlKind.LITERAL)){
                RexLiteral literal = (RexLiteral) node;
                LiteralColumn literalColumn = new LiteralColumn(CalciteUtils.getValue(literal), EMPTY_ORDINAL, outputFields.get(i), true);
                tableContainer.addProjectedColumn(literalColumn);
                tableContainer.getVisibleColumns().add(literalColumn);
            }
        }
    }

    private void addQueryColumns(RexCall call, List<IQueryColumn> queryColumns, List<String> inputFields, List<String> outputFields, int index) {
        for (RexNode operand : call.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = program.getExprList().get(((RexLocalRef) operand).getIndex());
                if (rexNode.isA(SqlKind.INPUT_REF)) {
                    RexInputRef rexInputRef = (RexInputRef) rexNode;
                    String column = inputFields.get(rexInputRef.getIndex());
                    queryColumns.add(tableContainer.addQueryColumnWithoutOrdinal(column, null, false));
                } else if (rexNode.isA(SqlKind.LITERAL)) {
                    RexLiteral literal = (RexLiteral) rexNode;
                    queryColumns.add(new LiteralColumn(CalciteUtils.getValue(literal), index, outputFields.get(index), false));
                } else if (rexNode instanceof RexCall) {
                    RexCall inner = (RexCall) rexNode;
                    switch (inner.getKind()) {
                        case EXTRACT:
                        case OTHER_FUNCTION:
                            SqlFunction sqlFunction = (SqlFunction) inner.op;
                            List<IQueryColumn> innerColumns = new ArrayList<>();
                            addQueryColumns((RexCall) rexNode, innerColumns, inputFields, outputFields, -1);
                            queryColumns.add(new FunctionColumn(session, innerColumns, sqlFunction.getName(),
                                    sqlFunction.toString(), null, false, EMPTY_ORDINAL, call.getType().getFullTypeString()));
                            break;
                    }
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
