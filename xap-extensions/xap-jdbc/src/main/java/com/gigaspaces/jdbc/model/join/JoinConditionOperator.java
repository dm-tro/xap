package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.result.TableRowUtils;
import org.apache.calcite.sql.SqlKind;

import java.util.Objects;

public enum JoinConditionOperator implements JoinCondition {
    EQ, NE, GT, GE, LT, LE, OR, AND;

    public static JoinCondition getCondition(SqlKind sqlKind) {
        switch (sqlKind) {
            case EQUALS:
                return EQ;
            case NOT_EQUALS:
                return NE;
            case GREATER_THAN:
                return GT;
            case GREATER_THAN_OR_EQUAL:
                return GE;
            case LESS_THAN:
                return LT;
            case LESS_THAN_OR_EQUAL:
                return LE;
            case OR:
                return OR;
            case AND:
                return AND;
            default:
                throw new UnsupportedOperationException("Join with sqlType " + sqlKind + " is not supported");
        }
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public boolean isOperator() {
        return true;
    }


    public boolean evaluate(Object leftValue, Object rightValue) {
        if (leftValue == null || rightValue == null) {
            return false;
        }
        Comparable first = TableRowUtils.castToComparable(rightValue);
        Comparable second = TableRowUtils.castToComparable(leftValue);
        int compareResult = first.compareTo(second);
        switch (this) {
            case EQ:
                return Objects.equals(leftValue, rightValue);
            case NE:
                return !Objects.equals(leftValue, rightValue);
            case GE:
                return compareResult >= 0;
            case GT:
                return compareResult > 0;
            case LE:
                return compareResult <= 0;
            case LT:
                return compareResult < 0;
            case AND:
                return (boolean) first && (boolean) second;
            case OR:
                return (boolean) first || (boolean) second;
            default:
                throw new UnsupportedOperationException("Join with operator " + this + " is not supported");
        }
    }
}
