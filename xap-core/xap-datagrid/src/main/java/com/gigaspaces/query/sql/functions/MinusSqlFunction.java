package com.gigaspaces.query.sql.functions;

import com.gigaspaces.internal.utils.ObjectConverter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Returns the the subtraction of two objects
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */
public class MinusSqlFunction extends SqlFunction {


    final static HashSet<String> monthIntervals = new HashSet<>(Arrays.asList("INTERVAL_YEAR", "INTERVAL_MONTH", "INTERVAL_YEAR_MONTH"));
    final static HashSet<String> nanoIntervals = new HashSet<>(Arrays.asList("INTERVAL_DAY", "INTERVAL_DAY_HOUR", "INTERVAL_DAY_MINUTE", "INTERVAL_DAY_SECOND", "INTERVAL_HOUR", "INTERVAL_HOUR_MINUTE", "INTERVAL_MINUTE",
            "INTERVAL_SECOND", "INTERVAL_HOUR_SECOND", "INTERVAL_MINUTE_SECOND"));

    /**
     * @param context which contains two arguments that have subtraction relations between them. also contains the type of the second object to support intervals
     * @return Returns the subtraction of the second object from the first one.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object left = context.getArgument(0);
        Object right = context.getArgument(1);

        if (left instanceof LocalTime) {
            if (right instanceof BigDecimal) {
                if (nanoIntervals.contains(context.getType())) {
                    int milliToNanoFactor = 1000 * 1000;
                    return ((LocalTime) left).minusNanos(((BigDecimal) right).longValue() * milliToNanoFactor);
                } else {
                    throw new RuntimeException("cannot subtract " + context.getType() + " from time");
                }
            }

        }
        if (left instanceof LocalDate) {
            if (right instanceof BigDecimal) {
                if (monthIntervals.contains(context.getType())) {
                    return ((LocalDate) left).minusMonths(((BigDecimal) right).longValue());
                } else if (context.getType().equals("INTERVAL_DAY")) {
                    long milliToDaysFactor = 1000 * 60 * 60 * 24;
                    return ((LocalDate) left).minusDays(((BigDecimal) right).longValue() / milliToDaysFactor );
                }
                else {
                    throw new RuntimeException("cannot subtract " + context.getType() + " from date");
                }
            }
        }
        if (left instanceof LocalDateTime) {
            if (right instanceof BigDecimal) {
                if (monthIntervals.contains(context.getType())) {
                    return ((LocalDateTime) left).minusMonths(((BigDecimal) right).longValue());
                } else if (nanoIntervals.contains(context.getType())){
                    int milliToNanoFactor = 1000 * 1000;
                    return ((LocalDateTime) left).minusNanos(((BigDecimal) right).longValue() * milliToNanoFactor);
                }
                else {
                    throw new RuntimeException("cannot subtract " + context.getType() + " from date");
                }
            }
        }
        throw new RuntimeException("cannot subtract " + left + " from " + right + " is not supported");
    }

}
