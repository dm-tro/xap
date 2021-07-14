package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.result.TableRowUtils;
import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.Objects;

public enum JoinConditionOperator implements JoinCondition {
    NOT, EQ, NE, GT, GE, LT, LE, LIKE, OR, AND;

    int numberOfOperands;

    public static JoinConditionOperator getCondition(SqlKind sqlKind) {
        switch (sqlKind) {
            case NOT:
                return NOT;
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
            case LIKE:
                return LIKE;
            case OR:
                return OR;
            case AND:
                return AND;
            default:
                throw new UnsupportedOperationException("Join with sqlType " + sqlKind + " is not supported");
        }
    }

    public int getNumberOfOperands() {
        return numberOfOperands;
    }

    public JoinConditionOperator setNumberOfOperands(int numberOfOperands) {
        this.numberOfOperands = numberOfOperands;
        return this;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public boolean isOperator() {
        return true;
    }

    public boolean evaluate(Object... values) {
        if (Arrays.stream(values).anyMatch(Objects::isNull)) {
            return false;
        }
        switch (this) {
            case NOT:
                return !(boolean) values[0];
            case EQ:
                return Objects.equals(values[0], values[1]);
            case NE:
                return !Objects.equals(values[0], values[1]);
            case GE:
                return getCompareResult(values[0], values[1]) >= 0;
            case GT:
                return getCompareResult(values[0], values[1]) > 0;
            case LE:
                return getCompareResult(values[0], values[1]) <= 0;
            case LT:
                return getCompareResult(values[0], values[1]) < 0;
            case AND:
                boolean ans = true;
                for (Object value : values) {
                    ans &= (boolean) value;
                }
                return ans;
            case OR:
                ans = false;
                for (Object value : values) {
                    ans |= (boolean) value;
                }
                return ans;
            case LIKE:
                // TODO: @sagiv try to use range?
                //                String regex = ((String) value).replaceAll("%", ".*").replaceAll("_", ".");
                //                range = isNot ? new NotRegexRange(column, regex) : new RegexRange(column, regex);
                return ((String) values[0]).matches(((String) values[1]).replaceAll("%", ".*").replaceAll("_", "."));
            default:
                throw new UnsupportedOperationException("Join with operator " + this + " is not supported");
        }
    }

    private int getCompareResult(Object leftValue, Object rightValue) {
        Comparable first = TableRowUtils.castToComparable(leftValue);
        Comparable second = TableRowUtils.castToComparable(rightValue);
        return first.compareTo(second);
    }
}
