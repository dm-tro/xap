package com.gigaspaces.sql.datagateway.netty.query;

import com.gigaspaces.jdbc.calcite.GSOptimizer;
import org.apache.calcite.sql.SqlNode;

public interface Statement extends AutoCloseable {
    /**
     * @return Statement name
     */
    String getName();

    /**
     * @return Parsed query
     */
    SqlNode getQuery();

    GSOptimizer getOptimizer();

    /**
     * @return Parameters and result row descriptions
     */
    StatementDescription getDescription();
}
