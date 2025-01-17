package com.gigaspaces.sql.datagateway.netty.query;

import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;

import java.util.List;

/**
 * Hides query execution internals, may be built
 * over a JDBC driver or exploit internal query API.
 */
public interface QueryProvider {
    /**
     * Executes multiline query.
     *
     * @param session Session
     * @param qry Query string.
     * @return List of query cursors.
     */
    List<Portal<?>> executeQueryMultiline(Session session, String qry) throws ProtocolException;

    /**
     * Prepares a query for future execution
     * @param session Session
     * @param stmt Statement name, named statement may be executed without
     *             parsing and validation. You should consider it as a prepared statement id.
     *             May be empty, in this case the statement will free resources and be destroyed
     *             right after execution
     * @param qry Query string.
     * @param paramTypes Inferred parameter types, an ODBC/JDBC driver does a query pre-parsing to
*                   identify query parameter types. May be empty, in this case server should
     */
    void prepare(Session session, String stmt, String qry, int[] paramTypes) throws ProtocolException;

    /**
     * Setups prepared statement parameters and binds the statement with
     * a portal - a PG abstraction describing server side cursor.
     * @param session Session
     * @param portal Portal name, or a server side cursor name.
     * @param stmt Statement name.
     * @param params Parameter values.
     * @param formatCodes Result format codes, it says how to serialize values - as text or as binary data.
     */
    void bind(Session session, String portal, String stmt, Object[] params, int[] formatCodes) throws ProtocolException;

    /**
     * Describes a statement, its parameters and result columns
     * @param stmt Describing statement name.
     * @return Statement description.
     */
    StatementDescription describeS(String stmt) throws ProtocolException;

    /**
     * Describes a portal and portal result columns.
     * @param portal Describing portal.
     * @return Portal description.
     */
    RowDescription describeP(String portal) throws ProtocolException;

    /**
     * Executes prepared and bind to a portal statement.
     * @param portal Portal name.
     * @return Result iterator.
     */
    Portal<?> execute(String portal) throws ProtocolException;

    /**
     * Closes a statement with given name and releases associated resources (including bound portals).
     *
     * @param stmt Statement name;
     */
    void closeS(String stmt) throws ProtocolException;

    /**
     * Closes a portal with given name and releases associated resources.
     *
     * @param portal Portal name.
     */
    void closeP(String portal) throws ProtocolException;

    /**
     * Cancels currently running query. Since in PostgreSQL all sessions are single-threaded,
     * session process cannot process messages while query executing, so that in order to cancel
     * a running query a new process started and cancels the query using query process id and session secret.
     * @param pid Process id.
     * @param secret Session secret.
     */
    void cancel(int pid, int secret);
}
