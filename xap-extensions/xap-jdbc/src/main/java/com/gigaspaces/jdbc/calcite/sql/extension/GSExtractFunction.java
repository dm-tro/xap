package com.gigaspaces.jdbc.calcite.sql.extension;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlExtractFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * An alternative to {@link SqlExtractFunction} that returns DOUBLE instead of BIGINT.
 */
public class GSExtractFunction extends SqlFunction {
    /** The original function to handle misc operations. */
    private static final SqlFunction ORIGINAL = new SqlExtractFunction();

    public GSExtractFunction() {
        super(
            "EXTRACT",
            SqlKind.EXTRACT,
            ReturnTypes.DOUBLE_NULLABLE,
            null,
            OperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
            SqlFunctionCategory.SYSTEM
        );
    }

    @Override public String getSignatureTemplate(int operandsCount) {
        return ORIGINAL.getSignatureTemplate(operandsCount);
    }

    @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        ORIGINAL.unparse(writer, call, leftPrec, rightPrec);
    }

    @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return ORIGINAL.getMonotonicity(call);
    }
}
