package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.handlers.*;
import com.gigaspaces.jdbc.calcite.pg.PgCalciteTable;
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.*;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectHandler extends RelShuttleImpl {
    private final QueryExecutor queryExecutor;
    private final Map<RelNode, GSCalc> childToCalc = new HashMap<>();
    private final LocalSession session;
    private RelNode root = null;
    private GSCalc rootCalc = null;

    public SelectHandler(QueryExecutor queryExecutor, LocalSession session) {
        this.queryExecutor = queryExecutor;
        this.session = session;
    }

    @Override
    // TODO check inserting of same table
    public RelNode visit(TableScan scan) {
        RelNode result = super.visit(scan);
        RelOptTable relOptTable = scan.getTable();
        GSTable gsTable = relOptTable.unwrap(GSTable.class);
        TableContainer tableContainer;
        if (gsTable != null) {
            tableContainer = new ConcreteTableContainer(gsTable.getName(), gsTable.getShortName(), queryExecutor.getSpace());
        } else {
            PgCalciteTable schemaTable = relOptTable.unwrap(PgCalciteTable.class);
            tableContainer = new SchemaTableContainer(schemaTable, null, queryExecutor.getSpace());
        }
        queryExecutor.getTables().add(tableContainer);
        if (childToCalc.containsKey(scan)) {
            tableContainer.setAllColumns(false);
            handleCalc(childToCalc.get(scan), tableContainer);
            childToCalc.remove(scan); // visited, not needed anymore
        }
        return result;
    }

    @Override
    public RelNode visit(RelNode other) {
        if(root == null){
            root = other;
        }
        if(other instanceof GSCalc){
            GSCalc calc = (GSCalc) other;
            if(rootCalc == null){
                rootCalc = calc;
            }
            RelNode input = calc.getInput();
            while (!(input instanceof GSJoin)
                    && !(input instanceof GSTableScan)
                    && !(input instanceof GSAggregate)) {
                if(input.getInputs().isEmpty()) {
                    break;
                }
                input =  input.getInput(0);
            }
            childToCalc.putIfAbsent(input, calc);
        }
        RelNode result = super.visit(other);
        if (other instanceof GSJoin) {
            handleJoin((GSJoin) other);
        } else if (other instanceof GSValues) {
            GSValues gsValues = (GSValues) other;
            handleValues(gsValues);
        } else if (other instanceof GSSort) {
            handleSort((GSSort) other);
        } else if(other instanceof GSAggregate){
            handleAggregate((GSAggregate) other);
        }

        return result;
    }

    private void handleValues(GSValues gsValues) {
        if (childToCalc.containsKey(gsValues)) {
            GSCalc gsCalc = childToCalc.get(gsValues);
            RexProgram program = gsCalc.getProgram();

            List<RexLocalRef> projectList = program.getProjectList();
            for (RexLocalRef project : projectList) {
                RexNode node = program.getExprList().get(project.getIndex());
                if (node instanceof RexCall) {
                    FunctionCallColumn functionCallColumn = getFunctionCallColumn(program, (RexCall) node);
                    queryExecutor.addColumn(functionCallColumn);
                }
            }
        }
    }

    private FunctionCallColumn getFunctionCallColumn(RexProgram program, RexCall rexCall) {
        SqlOperator sqlFunction = rexCall.op;
        List<IQueryColumn> params = new ArrayList<>();
        DateTypeResolver castType = new DateTypeResolver();
        for (RexNode operand : rexCall.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode funcArgument = program.getExprList().get(((RexLocalRef) operand).getIndex());
                if (funcArgument.isA(SqlKind.LITERAL)) {
                    RexLiteral literal = (RexLiteral) funcArgument;
                    int columnOrdinal = ((RexLocalRef) operand).getIndex();
                    params.add(new LiteralColumn(CalciteUtils.getValue(literal, castType), columnOrdinal));
                } else if (funcArgument instanceof RexCall) { //operator
                    RexCall function= (RexCall) funcArgument;
                    params.add(getFunctionCallColumn(program, function));
                }
            }

        }
        return new FunctionCallColumn(session, params, sqlFunction.getName(), null, null, true, -1, castType.getResolvedDateType());
    }

    private void handleSort(GSSort sort) {
        int columnCounter = 0;
        for (RelFieldCollation relCollation : sort.getCollation().getFieldCollations()) {
            int fieldIndex = relCollation.getFieldIndex();
            RelFieldCollation.Direction direction = relCollation.getDirection();
            RelFieldCollation.NullDirection nullDirection = relCollation.nullDirection;
            String columnAlias = sort.getRowType().getFieldNames().get(fieldIndex);
            String columnName = columnAlias;
            boolean isVisible = false;
            RelNode parent = this.stack.peek();
            if(parent instanceof GSCalc) {
                RexProgram program = ((GSCalc) parent).getProgram();
                RelDataTypeField field = program.getOutputRowType().getField(columnAlias, true, false);
                if(field != null) {
                    isVisible = true;
                    columnName = program.getInputRowType().getFieldNames().get(program.getSourceField(field.getIndex()));
                }
            }
            TableContainer table = queryExecutor.getTableByColumnName(columnName);
            OrderColumn orderColumn = new OrderColumn(new ConcreteColumn(columnName,null, columnAlias,
                    isVisible, table, columnCounter++), !direction.isDescending(),
                    nullDirection == RelFieldCollation.NullDirection.LAST);
            table.addOrderColumns(orderColumn);
        }
    }

    private void handleAggregate(GSAggregate gsAggregate) {
        AggregateHandler.instance().apply(gsAggregate, queryExecutor);
        if(childToCalc.containsKey(gsAggregate)){
            handleCalcFromAggregate(childToCalc.get(gsAggregate));
        }
    }

    private void handleCalcFromAggregate(GSCalc other){
        RexProgram program = other.getProgram();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        for (int i = 0; i < outputFields.size(); i++) {
            String outputField = outputFields.get(i);
            if(!outputField.startsWith("EXPR") && other.equals(rootCalc)){
                IQueryColumn qc = queryExecutor.getColumnByColumnName(outputField);
                queryExecutor.addColumn(qc);
            }
        }
        if (program.getCondition() != null) {
            ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, program.getInputRowType().getFieldNames());
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }

    private void handleJoin(GSJoin join) {
        RexCall rexCall = (RexCall) join.getCondition();
        JoinConditionHandler joinConditionHandler = new JoinConditionHandler(join, queryExecutor);
        TableContainer leftContainer = joinConditionHandler.handleRexCall(rexCall);
        if (leftContainer.getJoinedTable() != null && leftContainer.getJoinedTable().getJoinInfo() != null) {
            JoinInfo joinInfo = leftContainer.getJoinedTable().getJoinInfo();
            if (joinInfo.getJoinType().equals(JoinInfo.JoinType.LEFT) && !joinInfo.joinConditionsContainsOnlyEqualAndAndOperators()) {
                throw new UnsupportedOperationException("LEFT join only supports AND and EQUALS operators in ON condition");
            }
        }
        if(!childToCalc.containsKey(join)) { // it is SELECT *
            if(join.equals(root)
                    || ((root instanceof GSSort) && ((GSSort) root).getInput().equals(join))) { // root is GSSort and its child is join
                if (join.isSemiJoin()) {
                    queryExecutor.getVisibleColumns().addAll(leftContainer.getVisibleColumns());
                } else {
                    for (TableContainer tableContainer : queryExecutor.getTables()) {
                        queryExecutor.getVisibleColumns().addAll(tableContainer.getVisibleColumns());
                    }
                }
            }
        } else {
            handleCalcFromJoin(childToCalc.get(join));
            childToCalc.remove(join); // visited, not needed anymore
        }
    }

    private void handleCalc(GSCalc other, TableContainer tableContainer) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        new SingleTableProjectionHandler(session, program, tableContainer, other.equals(rootCalc), queryExecutor).project();
        ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields, tableContainer);
        if (program.getCondition() != null) {
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }

    private void handleCalcFromJoin(GSCalc other) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        List<RexLocalRef> projects = program.getProjectList();
        if(other.equals(rootCalc)) {
            for (int i = 0; i < projects.size(); i++) {
                RexLocalRef localRef = projects.get(i);
                RexNode node = program.getExprList().get(localRef.getIndex());
                switch (node.getKind()) {
                    case INPUT_REF: {
                        IQueryColumn qc = queryExecutor.getColumnByColumnIndex(program.getSourceField(i));
                        queryExecutor.addColumn(qc);
                        break;
                    }
                    case CASE: {
                        RexCall call = (RexCall) node;
                        CaseColumn caseColumn = new CaseColumn(outputFields.get(i), CalciteUtils.getJavaType(call), i);
                        CaseConditionHandler caseHandler = new CaseConditionHandler(program, queryExecutor, inputFields,
                                null, caseColumn);
                        caseHandler.visitCall(call);
                        queryExecutor.addCaseColumn(caseColumn);
                        break;
                    }
                    case OTHER_FUNCTION: {
                        RexCall call = (RexCall) node;
                        SqlFunction sqlFunction = (SqlFunction) call.op;
                        List<IQueryColumn> queryColumns = new ArrayList<>();
                        addQueryColumns(call, queryColumns, program, inputFields);
                        FunctionCallColumn functionCallColumn = new FunctionCallColumn(session, queryColumns, sqlFunction.getName(), sqlFunction.toString(), null, true, -1);
                        queryExecutor.addColumn(functionCallColumn);
                        break;
                    }
                    case LITERAL: {
                        RexLiteral literal = (RexLiteral) node;
                        int columnOrdinal = localRef.getIndex();
                        queryExecutor.addColumn(new LiteralColumn(CalciteUtils.getValue(literal), columnOrdinal));
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("Unexpected node kind expected CASE / INPUT_REF but was [" + node.getKind() + "]");
                }
            }
        }

        if (program.getCondition() != null) {
            ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }

    private void addQueryColumns(RexCall call, List<IQueryColumn> queryColumns, RexProgram program, List<String> inputFields) {

        for (RexNode operand : call.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = program.getExprList().get(((RexLocalRef) operand).getIndex());
                if (rexNode.isA(SqlKind.INPUT_REF)) {
                    RexInputRef rexInputRef = (RexInputRef) rexNode;
                    String column = inputFields.get(rexInputRef.getIndex());
                    TableContainer tableByColumnIndex = queryExecutor.getTableByColumnIndex(rexInputRef.getIndex());
                    queryColumns.add(tableByColumnIndex.addQueryColumn(column, null, false, -1));
                }
                else if (rexNode.isA(SqlKind.LITERAL)) {
                    RexLiteral literal = (RexLiteral) rexNode;
                    int columnOrdinal = ((RexLocalRef) operand).getIndex();
                    queryColumns.add(new LiteralColumn(CalciteUtils.getValue(literal), columnOrdinal));
                }
            }
        }
    }
}
