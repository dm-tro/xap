package com.gigaspaces.query.sql.functions;

import com.gigaspaces.internal.utils.ObjectConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Casts a String into the wanted data object
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */

public class CastSqlFunction extends SqlFunction {

    private static final Map<String, Class<?>> types = new HashMap<>();

    static {
        types.put("DOUBLE", Double.TYPE);
        types.put("FLOAT", Float.TYPE);
        types.put("INTEGER", Integer.TYPE);
        types.put("BIGINT", Long.TYPE);
        types.put("SHORT", Short.TYPE);
        types.put("TIMESTAMP", LocalDateTime.class);
        types.put("DATE", LocalDate.class);
        types.put("TIME", LocalTime.class);
        types.put("DECIMAL", BigDecimal.class);
    }

    /**
     * @param context contains one String argument and the type to cast to.
     * @return object of the wanted type.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, context);
        Object value = context.getArgument(0);
        if (!isString(value)) {
            throw new RuntimeException("Cast function - 1st argument must be a String: " + value);
        }
        String type = context.getType();
        try {
            if (type.startsWith("BOOLEAN")) {
                String boolValue = (String) value;
                if (boolValue.equalsIgnoreCase("t") || boolValue.equalsIgnoreCase("true"))
                    return true;
                if (boolValue.equalsIgnoreCase("f") || boolValue.equalsIgnoreCase("false"))
                    return false;
                throw new RuntimeException("Cast function - Invalid input: " + boolValue + " for data type: BOOLEAN");
            }
            //covers cases like when type = "TIME(0)"
            type= type.replaceAll("\\(\\d+\\)", "");
            if(types.get(type) == null){
                throw new RuntimeException("Cast function - casting to type " + type + " is not supported");
            }
            return ObjectConverter.convert(value, types.get(type));
        } catch (SQLException throwable) {
            throw new RuntimeException("Cast function - Invalid input: " + value + " for data type: " + type, throwable);
        }
    }
}
