package com.gigaspaces.jdbc.calcite;

import com.j_spaces.kernel.SystemProperties;

import java.util.Properties;

public class CalciteDefaults {


    public static final String DRIVER_KEY = SystemProperties.JDBC_DRIVER;
    public static final String DRIVER_VALUE = SystemProperties.JDBC_V3_DRIVER;

    public static final String SUPPORT_INEQUALITY = "com.gs.jdbc.v3.support.inequality";
    public static final String SUPPORT_SEMICOLON_SEPARATOR = "com.gs.jdbc.v3.support.semicolon_separator";
    public static final String SUPPORT_ROWNUM = "com.gs.jdbc.v3.support.rownum";
    public static final String SUPPORT_EXPLAIN_PLAN = "com.gs.jdbc.v3.support.explain_plan";


    public static boolean isCalciteDriverPropertySet() {
        return DRIVER_VALUE.equals(System.getProperty(DRIVER_KEY));
    }

    public static boolean isCalcitePropertySet(String key, Properties properties) {
        String value = System.getProperty(key);
        if (value == null && properties != null) {
            value = properties.getProperty(key);
        }

        return "true".equals(value);
    }

    public static void setCalciteDriverSystemProperty() {
        System.setProperty(DRIVER_KEY, DRIVER_VALUE);
    }
}