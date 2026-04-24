/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.driver.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.utils.AvaticaParameterBinder;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

/** Metadata handler for Arrow Flight. */
public class ArrowFlightMetaImpl extends MetaImpl {
  private final Map<StatementHandleKey, PreparedStatement> statementHandlePreparedStatementMap;

  /**
   * Constructs a {@link MetaImpl} object specific for Arrow Flight.
   *
   * @param connection A {@link AvaticaConnection}.
   */
  public ArrowFlightMetaImpl(final AvaticaConnection connection) {
    super(connection);
    this.statementHandlePreparedStatementMap = new ConcurrentHashMap<>();
    setDefaultConnectionProperties();
  }

  /** Construct a signature. */
  static Signature newSignature(final String sql, Schema resultSetSchema, Schema parameterSchema) {
    List<ColumnMetaData> columnMetaData =
        resultSetSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields());
    List<AvaticaParameter> parameters =
        parameterSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToAvaticaParameters(parameterSchema.getFields());
    StatementType statementType =
        resultSetSchema == null || resultSetSchema.getFields().isEmpty()
            ? StatementType.IS_DML
            : StatementType.SELECT;
    return new Signature(
        columnMetaData,
        sql,
        parameters,
        Collections.emptyMap(),
        null, // unnecessary, as SQL requests use ArrowFlightJdbcCursor
        statementType);
  }

  @Override
  public void closeStatement(final StatementHandle statementHandle) {
    PreparedStatement preparedStatement =
        statementHandlePreparedStatementMap.remove(new StatementHandleKey(statementHandle));
    // Testing if the prepared statement was created because the statement can be
    // not created until
    // this moment
    if (preparedStatement != null) {
      preparedStatement.close();
    }
  }

  @Override
  public void commit(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public ExecuteResult execute(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final long maxRowCount) {
    Preconditions.checkArgument(
        connection.id.equals(statementHandle.connectionId), "Connection IDs are not consistent");
    PreparedStatement preparedStatement = getPreparedStatement(statementHandle);

    if (preparedStatement == null) {
      throw new IllegalStateException("Prepared statement not found: " + statementHandle);
    }

    new AvaticaParameterBinder(
            preparedStatement, ((ArrowFlightConnection) connection).getBufferAllocator())
        .bind(typedValues);

    if (statementHandle.signature == null
        || statementHandle.signature.statementType == StatementType.IS_DML) {
      // Update query
      long updatedCount = preparedStatement.executeUpdate();
      return new ExecuteResult(
          Collections.singletonList(
              MetaResultSet.count(statementHandle.connectionId, statementHandle.id, updatedCount)));
    } else {
      // TODO Why is maxRowCount ignored?
      return new ExecuteResult(
          Collections.singletonList(
              MetaResultSet.create(
                  statementHandle.connectionId,
                  statementHandle.id,
                  true,
                  statementHandle.signature,
                  null)));
    }
  }

  @Override
  public ExecuteResult execute(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final int maxRowsInFirstFrame) {
    return execute(statementHandle, typedValues, (long) maxRowsInFirstFrame);
  }

  @Override
  public ExecuteBatchResult executeBatch(
      final StatementHandle statementHandle, final List<List<TypedValue>> parameterValuesList)
      throws IllegalStateException {
    Preconditions.checkArgument(
        connection.id.equals(statementHandle.connectionId), "Connection IDs are not consistent");
    PreparedStatement preparedStatement = getPreparedStatement(statementHandle);

    if (preparedStatement == null) {
      throw new IllegalStateException("Prepared statement not found: " + statementHandle);
    }

    final AvaticaParameterBinder binder =
        new AvaticaParameterBinder(
            preparedStatement, ((ArrowFlightConnection) connection).getBufferAllocator());
    for (int i = 0; i < parameterValuesList.size(); i++) {
      binder.bind(parameterValuesList.get(i), i);
    }

    // Update query
    long[] updatedCounts = {preparedStatement.executeUpdate()};
    return new ExecuteBatchResult(updatedCounts);
  }

  @Override
  public Frame fetch(
      final StatementHandle statementHandle, final long offset, final int fetchMaxRowCount) {
    /*
     * ArrowFlightMetaImpl does not use frames.
     * Instead, we have accessors that contain a VectorSchemaRoot with
     * the results.
     */
    throw AvaticaConnection.HELPER.wrap(
        String.format("%s does not use frames.", this), AvaticaConnection.HELPER.unsupported());
  }

  private PreparedStatement prepareForHandle(final String query, StatementHandle handle) {
    final PreparedStatement preparedStatement =
        ((ArrowFlightConnection) connection).getClientHandler().prepare(query);
    handle.signature =
        newSignature(
            query, preparedStatement.getDataSetSchema(), preparedStatement.getParameterSchema());
    statementHandlePreparedStatementMap.put(new StatementHandleKey(handle), preparedStatement);
    return preparedStatement;
  }

  @Override
  public StatementHandle prepare(
      final ConnectionHandle connectionHandle, final String query, final long maxRowCount) {
    final StatementHandle handle = super.createStatement(connectionHandle);
    prepareForHandle(query, handle);
    return handle;
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final StatementHandle statementHandle,
      final String query,
      final long maxRowCount,
      final PrepareCallback prepareCallback)
      throws NoSuchStatementException {
    return prepareAndExecute(
        statementHandle, query, maxRowCount, -1 /* Not used */, prepareCallback);
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final StatementHandle handle,
      final String query,
      final long maxRowCount,
      final int maxRowsInFirstFrame,
      final PrepareCallback callback)
      throws NoSuchStatementException {
    try {
      // Fast-path for non-query statements (INSERT/UPDATE/DELETE/DDL):
      // route through DoPutCommandStatementUpdate so we get an accurate update count
      // back from the server. The default prepared-statement path reports a non-empty
      // result-set schema for DML (GizmoSQL's DuckDB backend returns a Count column),
      // which makes Avatica treat the statement as a SELECT and leaves getUpdateCount()
      // at -1 — hiding execution statistics in downstream tools like DBeaver.
      if (isNonQueryStatement(query)) {
        final long updateCount =
            ((ArrowFlightConnection) connection).getClientHandler().executeUpdate(query);
        synchronized (callback.getMonitor()) {
          callback.clear();
          callback.assign(handle.signature, null, updateCount);
        }
        callback.execute();
        final MetaResultSet metaResultSet =
            MetaResultSet.count(handle.connectionId, handle.id, updateCount);
        return new ExecuteResult(Collections.singletonList(metaResultSet));
      }

      PreparedStatement preparedStatement = prepareForHandle(query, handle);
      final StatementType statementType = preparedStatement.getType();

      final long updateCount =
          statementType.equals(StatementType.UPDATE) ? preparedStatement.executeUpdate() : -1;
      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(handle.signature, null, updateCount);
      }
      callback.execute();
      final MetaResultSet metaResultSet =
          MetaResultSet.create(handle.connectionId, handle.id, false, handle.signature, null);
      return new ExecuteResult(Collections.singletonList(metaResultSet));
    } catch (SQLTimeoutException e) {
      // So far AvaticaStatement(executeInternal) only handles NoSuchStatement and
      // Runtime
      // Exceptions.
      throw new RuntimeException(e);
    } catch (SQLException e) {
      throw new NoSuchStatementException(handle);
    }
  }

  /**
   * Returns {@code true} if the given SQL is a non-query statement (INSERT/UPDATE/DELETE/MERGE or
   * DDL) that should be routed through {@code DoPutCommandStatementUpdate}. The check looks at the
   * leading SQL keyword after skipping leading whitespace and {@code --} / block comments.
   *
   * <p>A false positive would only mean we route a query through the update path and the server
   * rejects it; a false negative leaves the current (SELECT-like) behavior. SELECT, WITH, VALUES,
   * SHOW, PRAGMA, EXPLAIN, DESCRIBE are query-shaped and deliberately not treated as updates.
   *
   * <p>TODO: replace this heuristic once the Flight SQL spec adds an authoritative {@code
   * is_update} field on {@code ActionCreatePreparedStatementResult}. Tracking:
   *
   * <ul>
   *   <li>Spec PR: https://github.com/apache/arrow/pull/49498 (GH-49497)
   *   <li>Java impl PR: https://github.com/apache/arrow-java/pull/1064
   * </ul>
   *
   * Once upstream merges, have {@link #prepareAndExecute} check {@code
   * preparedStatementResult.hasIsUpdate() ? preparedStatementResult.getIsUpdate() :
   * isNonQueryStatement(query)} so we trust the server when it tells us, and fall back to this
   * keyword sniff only for older servers that don't populate the field. Known edge cases this
   * heuristic misses that the protocol field would fix: CTE-wrapped DML like {@code WITH t AS
   * (INSERT ... RETURNING *) SELECT ...} which starts with {@code WITH} but actually modifies data.
   */
  static boolean isNonQueryStatement(final String sql) {
    if (sql == null) {
      return false;
    }
    final int n = sql.length();
    int i = 0;
    while (i < n) {
      final char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
      } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        // line comment
        i += 2;
        while (i < n && sql.charAt(i) != '\n') {
          i++;
        }
      } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        // block comment
        i += 2;
        while (i + 1 < n && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
          i++;
        }
        i += 2;
      } else {
        break;
      }
    }
    if (i >= n) {
      return false;
    }
    int end = i;
    while (end < n && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_')) {
      end++;
    }
    final String keyword = sql.substring(i, end).toUpperCase();
    // Kept deliberately in sync with the ADBC Python driver's _DDL_DML_KEYWORDS
    // (src/adbc_driver_gizmosql/dbapi.py) so the JDBC and ADBC drivers classify
    // statements the same way. If you add or remove a keyword here, mirror the
    // change in the ADBC driver repo (gizmodata/adbc-driver-gizmosql).
    switch (keyword) {
      case "ALTER":
      case "ANALYZE":
      case "ATTACH":
      case "BEGIN":
      case "CALL":
      case "CHECKPOINT":
      case "COMMENT":
      case "COMMIT":
      case "COPY":
      case "CREATE":
      case "DELETE":
      case "DETACH":
      case "DROP":
      case "EXPORT":
      case "GRANT":
      case "IMPORT":
      case "INSERT":
      case "INSTALL":
      case "LOAD":
      case "MERGE":
      case "RENAME":
      case "REPLACE":
      case "RESET":
      case "REVOKE":
      case "ROLLBACK":
      case "SET":
      case "TRUNCATE":
      case "UPDATE":
      case "UPSERT":
      case "USE":
      case "VACUUM":
        return true;
      default:
        return false;
    }
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(
      final StatementHandle statementHandle, final List<String> queries)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public void rollback(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public boolean syncResults(
      final StatementHandle statementHandle, final QueryState queryState, final long offset)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return false;
  }

  @Override
  public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
    final ConnectionProperties result = super.connectionSync(ch, connProps);
    final String newCatalog = this.connProps.getCatalog();
    if (newCatalog != null) {
      try {
        ((ArrowFlightConnection) connection).getClientHandler().setCatalog(newCatalog);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  void setDefaultConnectionProperties() {
    // TODO Double-check this.
    connProps
        .setDirty(false)
        .setAutoCommit(true)
        .setReadOnly(true)
        .setCatalog(null)
        .setSchema(null)
        .setTransactionIsolation(Connection.TRANSACTION_NONE);
  }

  PreparedStatement getPreparedStatement(StatementHandle statementHandle) {
    return statementHandlePreparedStatementMap.get(new StatementHandleKey(statementHandle));
  }

  // Helper used to look up prepared statement instances later. Avatica doesn't
  // give us the
  // signature in
  // an UPDATE code path so we can't directly use StatementHandle as a map key.
  private static final class StatementHandleKey {
    public final String connectionId;
    public final int id;

    StatementHandleKey(StatementHandle statementHandle) {
      this.connectionId = statementHandle.connectionId;
      this.id = statementHandle.id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StatementHandleKey that = (StatementHandleKey) o;

      if (id != that.id) {
        return false;
      }
      return connectionId.equals(that.connectionId);
    }

    @Override
    public int hashCode() {
      int result = connectionId.hashCode();
      result = 31 * result + id;
      return result;
    }
  }
}
