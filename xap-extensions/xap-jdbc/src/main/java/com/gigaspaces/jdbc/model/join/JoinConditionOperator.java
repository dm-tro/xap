package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.result.TableRowUtils;
import org.apache.calcite.sql.SqlKind;

import java.util.Objects;

public class JoinConditionOperator implements JoinCondition {
    private final SqlKind sqlKind;
    private final int numberOfOperands;

    JoinConditionOperator(SqlKind sqlKind, int numberOfOperands) {
        this.sqlKind = sqlKind;
        this.numberOfOperands = numberOfOperands;
    }

    public static JoinConditionOperator getConditionOperator(SqlKind sqlKind, int numberOfOperands) {
        switch (sqlKind) {
            case IS_NULL:
            case IS_NOT_NULL:
            case NOT:
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case LIKE:
            case OR:
            case AND:
                return new JoinConditionOperator(sqlKind, numberOfOperands);
            default:
                throw new UnsupportedOperationException("Join with sqlType " + sqlKind + " is not supported");
        }
    }

    public int getNumberOfOperands() {
        return numberOfOperands;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
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
        switch (this.sqlKind) {
            case IS_NULL:
                return values[0] == null;
            case IS_NOT_NULL:
                return values[0] != null;
            case NOT:
                return values[0] != null && !(boolean) values[0];
            case EQUALS:
                return values[0] != null && values[1] != null && Objects.equals(values[0], values[1]);
            case NOT_EQUALS:
                return values[0] != null && values[1] != null && !Objects.equals(values[0], values[1]);
            case GREATER_THAN_OR_EQUAL:
                return values[0] != null && values[1] != null && getCompareResult(values[0], values[1]) >= 0;
            case GREATER_THAN:
                return values[0] != null && values[1] != null && getCompareResult(values[0], values[1]) > 0;
            case LESS_THAN_OR_EQUAL:
                return values[0] != null && values[1] != null && getCompareResult(values[0], values[1]) <= 0;
            case LESS_THAN:
                return values[0] != null && values[1] != null && getCompareResult(values[0], values[1]) < 0;
            case AND:
                boolean ans = true;
                for (Object value : values) {
                    if (value == null) {
                        return false;
                    }
                    ans &= (boolean) value;
                }
                return ans;
            case OR:
                ans = false;
                for (Object value : values) {
                    if (value == null) {
                        return false;
                    }
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

    @Override
    public String toString() {
        return sqlKind + "(" + numberOfOperands + ")";
    }
}
