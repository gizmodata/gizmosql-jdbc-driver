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

import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.replaceSemiColons;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.client.utils.FlightClientCache;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.DriverVersion;

/** Connection to the Arrow Flight server. */
public final class ArrowFlightConnection extends AvaticaConnection {

  private final BufferAllocator allocator;
  private final ArrowFlightSqlClientHandler clientHandler;
  private final ArrowFlightConnectionConfigImpl config;
  private ExecutorService executorService;
  private int metadataResultSetCount;
  private Map<Integer, ArrowFlightJdbcFlightStreamResultSet> metadataResultSetMap = new HashMap<>();

  /**
   * Creates a new {@link ArrowFlightConnection}.
   *
   * @param driver the {@link ArrowFlightJdbcDriver} to use.
   * @param factory the {@link AvaticaFactory} to use.
   * @param url the URL to use.
   * @param properties the {@link Properties} to use.
   * @param config the {@link ArrowFlightConnectionConfigImpl} to use.
   * @param allocator the {@link BufferAllocator} to use.
   * @param clientHandler the {@link ArrowFlightSqlClientHandler} to use.
   */
  private ArrowFlightConnection(
      final ArrowFlightJdbcDriver driver,
      final AvaticaFactory factory,
      final String url,
      final Properties properties,
      final ArrowFlightConnectionConfigImpl config,
      final BufferAllocator allocator,
      final ArrowFlightSqlClientHandler clientHandler) {
    super(driver, factory, url, properties);
    this.config = Preconditions.checkNotNull(config, "Config cannot be null.");
    this.allocator = Preconditions.checkNotNull(allocator, "Allocator cannot be null.");
    this.clientHandler = Preconditions.checkNotNull(clientHandler, "Handler cannot be null.");
    this.metadataResultSetCount = 0;
  }

  /**
   * Creates a new {@link ArrowFlightConnection} to a {@link FlightClient}.
   *
   * @param driver the {@link ArrowFlightJdbcDriver} to use.
   * @param factory the {@link AvaticaFactory} to use.
   * @param url the URL to establish the connection to.
   * @param properties the {@link Properties} to use for this session.
   * @param allocator the {@link BufferAllocator} to use.
   * @return a new {@link ArrowFlightConnection}.
   * @throws SQLException on error.
   */
  static ArrowFlightConnection createNewConnection(
      final ArrowFlightJdbcDriver driver,
      final AvaticaFactory factory,
      String url,
      final Properties properties,
      final BufferAllocator allocator)
      throws SQLException {
    url = replaceSemiColons(url);
    final ArrowFlightConnectionConfigImpl config = new ArrowFlightConnectionConfigImpl(properties);
    final ArrowFlightSqlClientHandler clientHandler =
        createNewClientHandler(config, allocator, driver.getDriverVersion());
    return new ArrowFlightConnection(
        driver, factory, url, properties, config, allocator, clientHandler);
  }

  private static ArrowFlightSqlClientHandler createNewClientHandler(
      final ArrowFlightConnectionConfigImpl config,
      final BufferAllocator allocator,
      final DriverVersion driverVersion)
      throws SQLException {
    try {
      return new ArrowFlightSqlClientHandler.Builder()
          .withHost(config.getHost())
          .withPort(config.getPort())
          .withUsername(config.getUser())
          .withPassword(config.getPassword())
          .withTrustStorePath(config.getTrustStorePath())
          .withTrustStorePassword(config.getTrustStorePassword())
          .withSystemTrustStore(config.useSystemTrustStore())
          .withTlsRootCertificates(config.getTlsRootCertificatesPath())
          .withClientCertificate(config.getClientCertificatePath())
          .withClientKey(config.getClientKeyPath())
          .withBufferAllocator(allocator)
          .withEncryption(config.useEncryption())
          .withDisableCertificateVerification(config.getDisableCertificateVerification())
          .withToken(config.getToken())
          .withCallOptions(config.toCallOption())
          .withRetainCookies(config.retainCookies())
          .withRetainAuth(config.retainAuth())
          .withCatalog(config.getCatalog())
          .withClientCache(config.useClientCache() ? new FlightClientCache() : null)
          .withConnectTimeout(config.getConnectTimeout())
          .withDriverVersion(driverVersion)
          .withOAuthConfiguration(config.getOauthConfiguration())
          .build();
    } catch (final SQLException e) {
      try {
        allocator.close();
      } catch (final Exception allocatorCloseEx) {
        e.addSuppressed(allocatorCloseEx);
      }
      throw e;
    }
  }

  void reset() throws SQLException {
    // Clean up any open Statements
    try {
      AutoCloseables.close(statementMap.values());
    } catch (final Exception e) {
      throw AvaticaConnection.HELPER.createException(e.getMessage(), e);
    }

    statementMap.clear();

    // Reset Holdability
    this.setHoldability(this.metaData.getResultSetHoldability());

    // Reset Meta
    ((ArrowFlightMetaImpl) this.meta).setDefaultConnectionProperties();
  }

  /**
   * Gets the client {@link #clientHandler} backing this connection.
   *
   * @return the handler.
   */
  ArrowFlightSqlClientHandler getClientHandler() {
    return clientHandler;
  }

  /**
   * Returns the CREATE VIEW DDL for the given view.
   *
   * <p>JDBC has no standard API for fetching view source, so this is a GizmoSQL-specific extension
   * reachable via {@link Connection#unwrap(Class)}. Preferred path is the server's {@code
   * _gizmosql_system.main.gizmosql_view_definition} catalog view (DuckDB backend, recent server
   * versions); on older servers where that catalog isn't registered we fall back to an inline query
   * against {@code duckdb_views()} — the same source the catalog view wraps.
   *
   * @param catalog exact-match catalog, or {@code null} to not filter
   * @param schema exact-match schema, or {@code null} to not filter
   * @param view required, exact-match view name
   * @return the DDL text, or {@code null} if the view is not found
   * @throws SQLException if neither the catalog view nor {@code duckdb_views()} is available —
   *     typically means the server is running the SQLite backend, which doesn't expose view DDL
   *     through this API
   */
  public String getViewDefinition(final String catalog, final String schema, final String view)
      throws SQLException {
    if (view == null) {
      return null;
    }
    // Preferred path: the catalog view (columns are JDBC-shaped).
    final String viewSql =
        "SELECT \"VIEW_DEFINITION\" FROM _gizmosql_system.main.gizmosql_view_definition"
            + " WHERE \"TABLE_NAME\" = '"
            + escapeSqlLiteral(view)
            + '\''
            + (catalog == null ? "" : " AND \"TABLE_CAT\" = '" + escapeSqlLiteral(catalog) + '\'')
            + (schema == null ? "" : " AND \"TABLE_SCHEM\" = '" + escapeSqlLiteral(schema) + '\'')
            + " LIMIT 1";
    try (Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(viewSql)) {
      return rs.next() ? rs.getString(1) : null;
    } catch (SQLException first) {
      if (!looksLikeMissingSystemCatalog(first)) {
        throw first;
      }
      // Fall through: server predates the _gizmosql_system catalog. Emulate the view inline.
    }

    // Fallback: query duckdb_views() directly (raw column names). This mirrors the catalog
    // view definition created at server startup — keep the two in sync if either changes.
    final String inlineSql =
        "SELECT sql FROM duckdb_views() WHERE view_name = '"
            + escapeSqlLiteral(view)
            + '\''
            + (catalog == null ? "" : " AND database_name = '" + escapeSqlLiteral(catalog) + '\'')
            + (schema == null ? "" : " AND schema_name = '" + escapeSqlLiteral(schema) + '\'')
            + " LIMIT 1";
    try (Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(inlineSql)) {
      return rs.next() ? rs.getString(1) : null;
    } catch (SQLException second) {
      final String msg = second.getMessage() == null ? "" : second.getMessage();
      if (msg.contains("duckdb_views") || msg.contains("Catalog Error")) {
        throw new SQLException(
            "getViewDefinition() is not supported on this GizmoSQL server: the DuckDB catalog"
                + " function duckdb_views() is unavailable, which typically means the server"
                + " is running the SQLite backend. Only the DuckDB backend currently exposes"
                + " view DDL through this API.",
            second);
      }
      throw second;
    }
  }

  private static boolean looksLikeMissingSystemCatalog(final Throwable t) {
    final String msg = t.getMessage() == null ? "" : t.getMessage();
    return msg.contains("_gizmosql_system")
        || msg.contains("gizmosql_view_definition")
        || msg.contains("Catalog Error");
  }

  private static String escapeSqlLiteral(final String value) {
    return value.replace("'", "''");
  }

  /**
   * Gets the {@link ExecutorService} of this connection.
   *
   * @return the {@link #executorService}.
   */
  synchronized ExecutorService getExecutorService() {
    return executorService =
        executorService == null
            ? Executors.newFixedThreadPool(
                config.threadPoolSize(), new DefaultThreadFactory(getClass().getSimpleName()))
            : executorService;
  }

  /**
   * Registers a new metadata ResultSet and assigns it a unique ID. Metadata ResultSets are those
   * created without an associated Statement.
   *
   * @param resultSet the ResultSet to register
   * @return the assigned ID
   */
  int getNewMetadataResultSetId(ArrowFlightJdbcFlightStreamResultSet resultSet) {
    metadataResultSetMap.put(metadataResultSetCount, resultSet);
    return metadataResultSetCount++;
  }

  /**
   * Unregisters a metadata ResultSet when it is closed. This method is called by metadata
   * ResultSets during their close operation to remove themselves from the tracking map.
   *
   * @param id the ID of the ResultSet to unregister, or null if not a metadata ResultSet
   */
  void onResultSetClose(Integer id) {
    if (id == null) {
      return;
    }
    metadataResultSetMap.remove(id);
  }

  @Override
  public Properties getClientInfo() {
    final Properties copy = new Properties();
    copy.putAll(info);
    return copy;
  }

  @Override
  public void close() throws SQLException {
    Exception topLevelException = null;
    try {
      if (executorService != null) {
        executorService.shutdown();
      }
    } catch (final Exception e) {
      topLevelException = e;
    }
    // copies of the collections are used to avoid concurrent modification problems
    ArrayList<AutoCloseable> closeables = new ArrayList<>(statementMap.values());
    closeables.addAll(new ArrayList<>(metadataResultSetMap.values()));
    closeables.add(clientHandler);
    closeables.addAll(allocator.getChildAllocators());
    closeables.add(allocator);
    try {
      AutoCloseables.close(closeables);
    } catch (final Exception e) {
      if (topLevelException == null) {
        topLevelException = e;
      } else {
        topLevelException.addSuppressed(e);
      }
    }
    try {
      super.close();
    } catch (final Exception e) {
      if (topLevelException == null) {
        topLevelException = e;
      } else {
        topLevelException.addSuppressed(e);
      }
    }
    if (topLevelException != null) {
      throw AvaticaConnection.HELPER.createException(
          topLevelException.getMessage(), topLevelException);
    }
  }

  BufferAllocator getBufferAllocator() {
    return allocator;
  }

  public ArrowFlightMetaImpl getMeta() {
    return (ArrowFlightMetaImpl) this.meta;
  }
}
