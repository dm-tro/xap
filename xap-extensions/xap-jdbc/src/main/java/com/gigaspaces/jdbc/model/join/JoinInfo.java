package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.j_spaces.jdbc.Stack;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.statement.select.Join;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.List;

public class JoinInfo {

    private final IQueryColumn leftColumn;
    private final IQueryColumn rightColumn;
    private final JoinType joinType;
    private final List<JoinCondition> joinConditions = new ArrayList<>();
    private Range range;
    private boolean hasMatch;

    public JoinInfo(IQueryColumn leftColumn, IQueryColumn rightColumn, JoinType joinType) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.joinType = joinType;
    }

    public JoinInfo(JoinType joinType) {
        this.leftColumn = null;
        this.rightColumn = null;
        this.joinType = joinType;
    }

    public boolean checkJoinCondition() {
        if (joinType.equals(JoinType.INNER) || joinType.equals(JoinType.SEMI)) {
            hasMatch = calculateConditions();
        } else if (range != null) {
            boolean found = false;
            for (JoinCondition joinCondition : joinConditions) {
                if (joinCondition instanceof JoinConditionColumnValue) {
                    IQueryColumn column = ((JoinConditionColumnValue) joinCondition).getColumn();
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
                JoinConditionOperator joinConditionOperator = (JoinConditionOperator) joinCondition;
                switch (joinConditionOperator) {
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE:
                    case LIKE:
                    case AND:
                    case OR:
                        JoinCondition first = stack.pop();
                        JoinCondition second = stack.pop();
                        boolean evaluate = joinConditionOperator.evaluate(first.getValue(), second.getValue());
                        stack.push(new JoinConditionBooleanValue(evaluate));
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

    public IQueryColumn getLeftColumn() {
        return leftColumn;
    }

    public IQueryColumn getRightColumn() {
        return rightColumn;
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

    public void resetHasMatch() {
        hasMatch = false;
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
