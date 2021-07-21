package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.j_spaces.jdbc.Stack;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.statement.select.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

public class JoinInfo {

    private final JoinType joinType;
    private final List<JoinCondition> joinConditions = new ArrayList<>();
    private final boolean isEqui;
    private Range range;
    private boolean hasMatch;

    public JoinInfo(JoinType joinType, boolean isEqui) {
        this.joinType = joinType;
        this.isEqui = isEqui;
    }

    public boolean checkJoinCondition() {
        if (joinType.equals(JoinType.LEFT)) {
            if (range != null) {
                boolean found = false;
                for (JoinCondition joinCondition : joinConditions) {
                    if (joinCondition instanceof ColumnValueJoinCondition) {
                        IQueryColumn column = ((ColumnValueJoinCondition) joinCondition).getColumn();
                        if (range.getPath().equals(column.getName())) {
                            hasMatch = range.getPredicate().execute(column.getCurrentValue());
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    hasMatch = false;
                }
            } else { //match from HashCursor
                hasMatch = true;
            }
        } else if (joinType.equals(JoinType.INNER) || joinType.equals(JoinType.SEMI)) {
            hasMatch = calculateConditions();
        } else {
            hasMatch = true;
        }
        return hasMatch;
    }

    private boolean calculateConditions() {
        Stack<JoinCondition> stack = new Stack<>();
        for (int i = joinConditions.size() - 1; i >= 0; i--) {
            JoinCondition joinCondition = joinConditions.get(i);
            if (joinCondition.isOperator()) {
                OperatorJoinCondition operatorJoinCondition = (OperatorJoinCondition) joinCondition;
                boolean evaluate;
                switch (operatorJoinCondition.getSqlKind()) {
                    case NOT:
                    case IS_NULL:
                    case IS_NOT_NULL:
                        evaluate = operatorJoinCondition.evaluate(stack.pop().getValue());
                        stack.push(new BooleanValueJoinCondition(evaluate));
                        break;
                    case EQUALS:
                    case NOT_EQUALS:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LIKE:
                        evaluate = operatorJoinCondition.evaluate(stack.pop().getValue(), stack.pop().getValue());
                        stack.push(new BooleanValueJoinCondition(evaluate));
                        break;
                    case AND:
                    case OR:
                        int numberOfOperands = operatorJoinCondition.getNumberOfOperands();
                        Object[] values = new Object[numberOfOperands];
                        for (int j = 0; j < numberOfOperands; j++) {
                            values[j] = stack.pop().getValue();
                        }
                        evaluate = operatorJoinCondition.evaluate(values);
                        stack.push(new BooleanValueJoinCondition(evaluate));
                        break;
                    default:
                        throw new UnsupportedOperationException("Join with operator " + joinCondition + " is not supported");

                }
            } else {
                stack.push(joinCondition);
            }
        }
        return (boolean) stack.pop().getValue();
    }

    public void addJoinCondition(JoinCondition joinCondition) {
        this.joinConditions.add(joinCondition);
    }

    public boolean isHasMatch() {
        return hasMatch;
    }

    public void resetHasMatch() {
        this.hasMatch = false;
    }

    public boolean isEquiJoin() {
        return isEqui;
    }

    public List<JoinCondition> getJoinConditions() {
        return joinConditions;
    }

    public boolean joinConditionsContainsOnlyEqualAndAndOperators() {
        for (JoinCondition joinCondition : joinConditions) {
            if (joinCondition.isOperator()) {
                OperatorJoinCondition operatorJoinCondition = (OperatorJoinCondition) joinCondition;
                SqlKind sqlKind = operatorJoinCondition.getSqlKind();
                if (!sqlKind.equals(SqlKind.AND) && !sqlKind.equals(SqlKind.EQUALS)) {
                    return false;
                }
            }
        }
        return true;
    }

    public IQueryColumn getLeftColumn() {
        if (isEqui) {
            return ((ColumnValueJoinCondition) this.joinConditions.get(2)).getColumn();
        }
        return null;
    }

    public IQueryColumn getRightColumn() {
        if (isEqui) {
            return ((ColumnValueJoinCondition) this.joinConditions.get(1)).getColumn();
        }
        return null;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public boolean insertRangeToJoinInfo(Range range) {
        if (joinType.equals(JoinType.RIGHT) || joinType.equals(JoinType.LEFT)) {
            this.range = range;
            return true;
        }
        return false;
    }

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL, SEMI;

        public static JoinType getType(Join join) {
            if (join.isLeft())
                return LEFT;
            if (join.isRight())
                return RIGHT;
            if (join.isOuter() || join.isFull())
                return FULL;
            if (join.isSemi()) {
                return SEMI;
            }
            return INNER;
        }

        public static JoinType getType(JoinRelType joinRelType) {
            switch (joinRelType) {
                case INNER:
                    return INNER;
                case LEFT:
                    return LEFT;
                case RIGHT:
                    return RIGHT;
                case FULL:
                    return FULL;
                case SEMI:
                    return SEMI;
                default:
                    throw new UnsupportedOperationException("Join of type " + joinRelType + " is not supported");
            }
        }

        public static byte toCode(JoinType joinType) {
            if (joinType == null)
                return 0;
            switch (joinType) {
                case INNER:
                    return 1;
                case LEFT:
                    return 2;
                case RIGHT:
                    return 3;
                case FULL:
                    return 4;
                case SEMI:
                    return 5;
                default:
                    throw new IllegalArgumentException("Unsupported join type: " + joinType);
            }
        }

        public static JoinType fromCode(byte code) {
            switch (code) {
                case 0:
                    return null;
                case 1:
                    return INNER;
                case 2:
                    return LEFT;
                case 3:
                    return RIGHT;
                case 4:
                    return FULL;
                case 5:
                    return SEMI;
                default:
                    throw new IllegalArgumentException("Unsupported join code: " + code);
            }
        }
    }

    public enum JoinAlgorithm {
        Nested, Hash, SortMerge
    }
}
