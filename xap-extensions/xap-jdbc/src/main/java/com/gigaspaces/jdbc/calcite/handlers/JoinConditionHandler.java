package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.GSJoin;
import com.gigaspaces.jdbc.model.join.JoinConditionColumnValue;
import com.gigaspaces.jdbc.model.join.JoinConditionOperator;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;

public class JoinConditionHandler {
    private final GSJoin join;
    private final QueryExecutor queryExecutor;
    private final JoinInfo joinInfo;

    public JoinConditionHandler(GSJoin join, QueryExecutor queryExecutor) {
        this.join = join;
        this.queryExecutor = queryExecutor;
        this.joinInfo = new JoinInfo(JoinInfo.JoinType.getType(join.getJoinType()));
    }


    public TableContainer handleRexCall(RexCall call){
        TableContainer leftContainer;
        switch (call.getKind()) {
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                leftContainer = handleSingleJoinCondition(join, call);
                break;
            case OR:
            case AND:
                joinInfo.addJoinCondition(JoinConditionOperator.getCondition(call.getKind()));
                leftContainer = handleSingleJoinCondition(join, (RexCall) call.getOperands().get(0));
                leftContainer = handleSingleJoinCondition(join, (RexCall) call.getOperands().get(1));
                break;
            default:
                throw new UnsupportedOperationException("Join condition type [" + call.getKind() + "] is not supported");
        }
        return leftContainer;
    }


    private TableContainer handleSingleJoinCondition(GSJoin join, RexCall rexCall) {
        switch (rexCall.getKind()) {
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                break;
            case OR:
            case AND:
                return handleRexCall(rexCall);
            default:
                throw new UnsupportedOperationException("Join condition type [" + rexCall.getKind() + "] is not supported");
        }
        int firstOperandIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
        int secondOperandIndex = ((RexInputRef) rexCall.getOperands().get(1)).getIndex();
        int leftIndex;
        int rightIndex;
        int diff = join.getLeft().getRowType().getFieldCount();
        if (firstOperandIndex < secondOperandIndex) {
            leftIndex = firstOperandIndex;
            rightIndex = secondOperandIndex;
        } else {
            leftIndex = secondOperandIndex;
            rightIndex = firstOperandIndex;
        }
        String lColumn = join.getLeft().getRowType().getFieldNames().get(leftIndex);
        String rColumn = join.getRight().getRowType().getFieldNames().get(rightIndex - diff);
        TableContainer rightContainer = queryExecutor.getTableByColumnIndex(rightIndex);
        TableContainer leftContainer = queryExecutor.getTableByColumnIndex(leftIndex);
        //TODO: @sagiv needed?- its already in the tables.
        IQueryColumn rightColumn = rightContainer.addQueryColumn(rColumn, null, false, -1);
        IQueryColumn leftColumn = leftContainer.addQueryColumn(lColumn, null, false, -1);
        if(rightContainer.getJoinInfo() == null) {
            rightContainer.setJoinInfo(joinInfo);
        }
        joinInfo.addJoinCondition(JoinConditionOperator.getCondition(rexCall.getKind()));
        joinInfo.addJoinCondition(new JoinConditionColumnValue(leftColumn));
        joinInfo.addJoinCondition(new JoinConditionColumnValue(rightColumn));

        if (leftContainer.getJoinedTable() == null) {
            if (!rightContainer.isJoined()) {
                leftContainer.setJoinedTable(rightContainer);
                rightContainer.setJoined(true);
            }
        }
        return leftContainer;
    }
}
