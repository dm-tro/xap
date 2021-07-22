package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.jdbc.calcite.GSOptimizer;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;

public class ExplainStatement extends StatementImpl {
    private static final StatementDescription DESCRIPTION = new StatementDescription(
            ParametersDescription.EMPTY,
            new RowDescription(ImmutableList.of(
                    new ColumnDescription("explain", TypeUtils.PG_TYPE_VARCHAR))));

    private final SqlExplain.Depth depth;
    private final SqlExplainLevel detailLevel;
    private final SqlExplainFormat format;
    private final RelDataType rowType;

    public ExplainStatement(QueryProviderImpl queryProvider, String name, SqlExplain.Depth depth, SqlExplainLevel detailLevel, SqlExplainFormat format, RelDataType rowType, SqlNode explicandum, GSOptimizer optimizer) {
        super(queryProvider, name, explicandum, optimizer, DESCRIPTION);
        this.depth = depth;
        this.detailLevel = detailLevel;
        this.format = format;
        this.rowType = rowType;
    }

    public SqlExplain.Depth getDepth() {
        return depth;
    }

    public SqlExplainLevel getDetailLevel() {
        return detailLevel;
    }

    public SqlExplainFormat getFormat() {
        return format;
    }

    public RelDataType getRowType() {
        return rowType;
    }
}
