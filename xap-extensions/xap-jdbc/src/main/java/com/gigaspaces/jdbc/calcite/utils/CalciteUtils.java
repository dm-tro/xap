package com.gigaspaces.jdbc.calcite.utils;

import com.gigaspaces.jdbc.calcite.CalciteDefaults;
import com.gigaspaces.jdbc.calcite.DateTypeResolver;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.math.BigDecimal;
import java.util.Properties;

import static com.gigaspaces.jdbc.calcite.CalciteDefaults.isCalcitePropertySet;

public class CalciteUtils {

    private static final String EXPLAIN_PREFIX = "explain";
    private static final String SELECT_PREFIX = "select";

    public static Object getValue(RexLiteral literal) {
        return getValue(literal, null);
    }

    public static Object getValue(RexLiteral literal, DateTypeResolver dateTypeResolver) {
        if (literal == null) {
            return null;
        }
        switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN:
                return RexLiteral.booleanValue(literal);
            case CHAR:
            case VARCHAR:
                return literal.getValueAs(String.class);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                //avoid returning BigDecimal
                return literal.getValue2();
            case REAL:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return literal.getValue3();
            case DATE:
            case TIMESTAMP:
            case TIME_WITH_LOCAL_TIME_ZONE:
                if (dateTypeResolver != null) dateTypeResolver.setResolvedDateType(literal);
                return literal.toString(); // we use our parsers with AbstractParser.parse
            case TIME:
                if (dateTypeResolver != null) dateTypeResolver.setResolvedDateType(literal);
                String[] strings = literal.toString().split(":");
                return String.join(":", strings[0], strings[1], strings[2]);
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
            case SYMBOL:
                return literal.getValue();
            default:
                throw new UnsupportedOperationException("Unsupported type: " + literal.getType().getSqlTypeName());
        }
    }


    public static Class<?> getJavaType(RexNode rexNode) {
        if (rexNode == null) {
            return null;
        }
        switch (rexNode.getType().getSqlTypeName()) {
            case BOOLEAN:
                return Boolean.class;
            case CHAR:
            case VARCHAR:
                return String.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case REAL:
            case DECIMAL:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return BigDecimal.class;
            case DATE:
                return java.time.LocalDate.class;
            case TIMESTAMP:
                return java.time.LocalDateTime.class;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return java.time.ZonedDateTime.class; // TODO: @sagiv validate.
            case TIME:
                return java.time.LocalTime.class;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + rexNode.getType().getSqlTypeName());
        }

    }

    /**
     * Based on the system property or custom Space property, we parse the query
     * and adapt it to calcite notation. We do this only if the property is set,
     * in order to avoid performance penalty of String manipulation.
     */
    public static String prepareQueryForCalcite(String query, Properties properties) {
        //support for ; at end of statement - more than one statement is not supported.
        if (isCalcitePropertySet(CalciteDefaults.SUPPORT_SEMICOLON_SEPARATOR, properties)) {
            if (query.endsWith(";")) {
                query = query.replaceFirst(";", "");
            }
        }
        //support for != instead of <>
        if (isCalcitePropertySet(CalciteDefaults.SUPPORT_INEQUALITY, properties)) {
            query = query.replaceAll("!=", "<>");
        }
        //replace rownum with row_number() if needed
        if (isCalcitePropertySet(CalciteDefaults.SUPPORT_ROWNUM, properties)) {
            query = replaceRowNum( query );
        }
        if (isCalcitePropertySet(CalciteDefaults.SUPPORT_EXPLAIN_PLAN, properties)) {
            if (query.toLowerCase().replaceAll("\\s+", "").startsWith(EXPLAIN_PREFIX + SELECT_PREFIX)) {
                String queryAfterSelect = query.substring(query.toLowerCase().indexOf(SELECT_PREFIX) + SELECT_PREFIX.length());
                query = "EXPLAIN PLAN FOR SELECT" + queryAfterSelect;
            }
        }
        return query;
    }

    public static String replaceRowNum(String query) {
        return query.replaceAll(" (?i)rownum", " row_number()");
    }



}

