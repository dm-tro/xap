package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.rex.RexLiteral;


/**
 * For saving types of date in order to cast them to the right type later
 *
 * @Author: Mishel Liberman
 * @since 16.0
 */
public class DateTypeResolver {
    private String resolvedDateType;

    public DateTypeResolver() {};

    public String getResolvedDateType() {
        return resolvedDateType;
    }

    public void setResolvedDateType(RexLiteral literal) {
        this.resolvedDateType = literal.getType().getSqlTypeName().getName();
    }
}
