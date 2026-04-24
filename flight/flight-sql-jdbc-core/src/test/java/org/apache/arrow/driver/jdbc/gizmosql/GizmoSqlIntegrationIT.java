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
package org.apache.arrow.driver.jdbc.gizmosql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Integration tests for the GizmoSQL JDBC Driver.
 *
 * <p>These tests require a running GizmoSQL server. Configuration is read from environment
 * variables:
 *
 * <ul>
 *   <li>GIZMOSQL_HOST - Server hostname (default: localhost)
 *   <li>GIZMOSQL_PORT - Server port (default: 31337)
 *   <li>GIZMOSQL_USERNAME - Username for authentication
 *   <li>GIZMOSQL_PASSWORD - Password for authentication
 *   <li>GIZMOSQL_USE_TLS - Whether to use TLS (default: false)
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GizmoSqlIntegrationIT {

  private static String jdbcUrl;
  private static Properties connectionProps;
  private static boolean serverAvailable = false;

  @BeforeAll
  static void setUp() {
    String host = System.getenv().getOrDefault("GIZMOSQL_HOST", "localhost");
    String port = System.getenv().getOrDefault("GIZMOSQL_PORT", "31337");
    String username = System.getenv().getOrDefault("GIZMOSQL_USERNAME", "gizmosql_user");
    String password = System.getenv().getOrDefault("GIZMOSQL_PASSWORD", "gizmosql_password");
    boolean useTls =
        Boolean.parseBoolean(System.getenv().getOrDefault("GIZMOSQL_USE_TLS", "false"));

    // Use the new gizmosql:// scheme
    jdbcUrl = String.format("jdbc:gizmosql://%s:%s", host, port);

    connectionProps = new Properties();
    connectionProps.setProperty("user", username);
    connectionProps.setProperty("password", password);
    connectionProps.setProperty("useEncryption", String.valueOf(useTls));
    if (!useTls) {
      connectionProps.setProperty("disableCertificateVerification", "true");
    }

    // Check if server is available
    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
      serverAvailable = conn != null && !conn.isClosed();
    } catch (SQLException e) {
      System.err.println("GizmoSQL server not available at " + jdbcUrl + ": " + e.getMessage());
      serverAvailable = false;
    }
  }

  @AfterAll
  static void tearDown() {
    // Cleanup if needed
  }

  private void assumeServerAvailable() {
    assumeTrue(serverAvailable, "GizmoSQL server not available - skipping test");
  }

  @Test
  @Order(1)
  void testConnection() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
      assertNotNull(conn);
      assertFalse(conn.isClosed());
    }
  }

  @Test
  @Order(2)
  void testDatabaseMetaData() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
      DatabaseMetaData metaData = conn.getMetaData();
      assertNotNull(metaData);

      // Check driver info
      String driverName = metaData.getDriverName();
      assertNotNull(driverName);
      System.out.println("Driver Name: " + driverName);

      String driverVersion = metaData.getDriverVersion();
      assertNotNull(driverVersion);
      System.out.println("Driver Version: " + driverVersion);

      // Check database info
      String productName = metaData.getDatabaseProductName();
      assertNotNull(productName);
      System.out.println("Database Product: " + productName);

      String productVersion = metaData.getDatabaseProductVersion();
      assertNotNull(productVersion);
      System.out.println("Database Version: " + productVersion);
    }
  }

  @Test
  @Order(3)
  void testSimpleQuery() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 AS value")) {

      assertTrue(rs.next());
      assertEquals(1, rs.getInt("value"));
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(4)
  void testQueryWithMultipleRows() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")) {

      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        int id = rs.getInt(1);
        String name = rs.getString(2);
        assertTrue(id >= 1 && id <= 3);
        assertNotNull(name);
      }
      assertEquals(3, rowCount);
    }
  }

  @Test
  @Order(5)
  void testCreateAndQueryTable() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement()) {

      // Create a test table
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR, value DOUBLE)");

      // Insert some data
      stmt.execute("INSERT INTO test_table VALUES (1, 'Alice', 10.5)");
      stmt.execute("INSERT INTO test_table VALUES (2, 'Bob', 20.0)");
      stmt.execute("INSERT INTO test_table VALUES (3, 'Charlie', 30.5)");

      // Query the data
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table ORDER BY id")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));
        assertEquals(10.5, rs.getDouble("value"), 0.001);

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("Bob", rs.getString("name"));

        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertEquals("Charlie", rs.getString("name"));

        assertFalse(rs.next());
      }

      // Cleanup
      stmt.execute("DROP TABLE test_table");
    }
  }

  @Test
  @Order(6)
  void testPreparedStatement() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement()) {

      // Create table
      stmt.execute("CREATE TABLE IF NOT EXISTS prepared_test (id INTEGER, name VARCHAR)");
      stmt.execute("INSERT INTO prepared_test VALUES (1, 'Test1'), (2, 'Test2'), (3, 'Test3')");

      // Use prepared statement with parameter
      try (PreparedStatement pstmt =
          conn.prepareStatement("SELECT * FROM prepared_test WHERE id > ?")) {
        pstmt.setInt(1, 1);

        try (ResultSet rs = pstmt.executeQuery()) {
          int count = 0;
          while (rs.next()) {
            count++;
            assertTrue(rs.getInt("id") > 1);
          }
          assertEquals(2, count);
        }
      }

      // Cleanup
      stmt.execute("DROP TABLE prepared_test");
    }
  }

  @Test
  @Order(7)
  void testResultSetMetaData() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery("SELECT 42 AS int_col, 'hello' AS str_col, 3.14 AS double_col")) {

      ResultSetMetaData metaData = rs.getMetaData();
      assertNotNull(metaData);

      assertEquals(3, metaData.getColumnCount());

      assertEquals("int_col", metaData.getColumnName(1));
      assertEquals("str_col", metaData.getColumnName(2));
      assertEquals("double_col", metaData.getColumnName(3));
    }
  }

  @Test
  @Order(8)
  void testNullHandling() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT NULL AS null_col, 42 AS int_col")) {

      assertTrue(rs.next());
      rs.getObject("null_col");
      assertTrue(rs.wasNull());
      assertEquals(42, rs.getInt("int_col"));
      assertFalse(rs.wasNull());
    }
  }

  @Test
  @Order(9)
  void testLargeResultSet() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM range(10000) AS t(i)")) {

      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(10000, count);
    }
  }

  @Test
  @Order(10)
  void testAggregation() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT COUNT(*) AS cnt, SUM(i) AS total, AVG(i) AS average "
                    + "FROM range(100) AS t(i)")) {

      assertTrue(rs.next());
      assertEquals(100, rs.getLong("cnt"));
      assertEquals(4950, rs.getLong("total")); // sum of 0..99
      assertEquals(49.5, rs.getDouble("average"), 0.001);
    }
  }

  @Test
  @Order(11)
  void testTransactionIsolation() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
      // Check that we can get transaction isolation level
      int isolation = conn.getTransactionIsolation();
      assertTrue(
          isolation == Connection.TRANSACTION_NONE
              || isolation == Connection.TRANSACTION_READ_UNCOMMITTED
              || isolation == Connection.TRANSACTION_READ_COMMITTED
              || isolation == Connection.TRANSACTION_REPEATABLE_READ
              || isolation == Connection.TRANSACTION_SERIALIZABLE);
    }
  }

  @Test
  @Order(12)
  void testGetCatalogs() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
      DatabaseMetaData metaData = conn.getMetaData();

      try (ResultSet rs = metaData.getCatalogs()) {
        // Just verify the query executes - catalogs may or may not exist
        assertNotNull(rs);
        while (rs.next()) {
          String catalog = rs.getString("TABLE_CAT");
          System.out.println("Catalog: " + catalog);
        }
      }
    }
  }

  @Test
  @Order(13)
  void testGetSchemas() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
      DatabaseMetaData metaData = conn.getMetaData();

      try (ResultSet rs = metaData.getSchemas()) {
        assertNotNull(rs);
        while (rs.next()) {
          String schema = rs.getString("TABLE_SCHEM");
          System.out.println("Schema: " + schema);
        }
      }
    }
  }

  @Test
  @Order(14)
  void testGetTables() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement()) {

      // Create a test table
      stmt.execute("CREATE TABLE IF NOT EXISTS get_tables_test (id INTEGER)");

      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getTables(null, null, "get_tables_test", null)) {
        assertNotNull(rs);
        assertTrue(rs.next(), "Should find the test table");
        String tableName = rs.getString("TABLE_NAME");
        assertEquals("get_tables_test", tableName.toLowerCase());
      }

      // Cleanup
      stmt.execute("DROP TABLE get_tables_test");
    }
  }

  // ===== Tests for Issue #95: Array literal syntax =====
  // https://github.com/gizmodata/gizmosql/issues/95
  //
  // These tests verify that DuckDB array syntax works correctly through the JDBC driver.
  // The issue occurs when tools like DBeaver transform [2,3,4] to ARRAY(2,3,4).

  @Test
  @Order(100)
  void testArrayLiteralBracketSyntax() throws SQLException {
    // Issue #95: Verify that DuckDB's bracket syntax for arrays works
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT [2, 3, 4] AS arr")) {

      assertTrue(rs.next());
      Object arr = rs.getObject("arr");
      assertNotNull(arr, "Array should not be null");
      System.out.println("Array result type: " + arr.getClass().getName());
      System.out.println("Array result: " + arr);
    }
  }

  @Test
  @Order(101)
  void testArrayLiteralListSyntax() throws SQLException {
    // Test DuckDB's list_value function as alternative
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT list_value(2, 3, 4) AS arr")) {

      assertTrue(rs.next());
      Object arr = rs.getObject("arr");
      assertNotNull(arr, "Array should not be null");
    }
  }

  @Test
  @Order(102)
  void testListHasAnyFunction() throws SQLException {
    // Issue #95: This was the specific use case that triggered the issue
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement()) {

      // Create a table with a list column
      stmt.execute("CREATE TABLE IF NOT EXISTS list_test (id INTEGER, anomalies INTEGER[])");
      stmt.execute("INSERT INTO list_test VALUES (1, [0, 1, 2])");
      stmt.execute("INSERT INTO list_test VALUES (2, [3, 4, 5])");
      stmt.execute("INSERT INTO list_test VALUES (3, [0, 6, 7])");

      // Query using list_has_any with bracket syntax
      try (ResultSet rs =
          stmt.executeQuery("SELECT id FROM list_test WHERE list_has_any(anomalies, [0])")) {
        int count = 0;
        while (rs.next()) {
          int id = rs.getInt("id");
          assertTrue(id == 1 || id == 3, "Should return rows with id 1 or 3");
          count++;
        }
        assertEquals(2, count, "Should find 2 rows with anomaly 0");
      }

      // Cleanup
      stmt.execute("DROP TABLE list_test");
    }
  }

  @Test
  @Order(103)
  void testNestedArrays() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT [[1, 2], [3, 4]] AS nested_arr")) {

      assertTrue(rs.next());
      Object arr = rs.getObject("nested_arr");
      assertNotNull(arr, "Nested array should not be null");
    }
  }

  @Test
  @Order(104)
  void testArrayAggregation() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery("SELECT list(i) AS arr FROM (SELECT unnest([1,2,3,4,5]) AS i)")) {

      assertTrue(rs.next());
      Object arr = rs.getObject("arr");
      assertNotNull(arr, "Aggregated array should not be null");
    }
  }

  @Test
  @Order(105)
  void testArrayWithMixedTypes() throws SQLException {
    // Test that string arrays work
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ['a', 'b', 'c'] AS str_arr")) {

      assertTrue(rs.next());
      Object arr = rs.getObject("str_arr");
      assertNotNull(arr, "String array should not be null");
    }
  }

  /**
   * Regression test for the DECIMAL prepared-statement parameter crash (v1.6.0). Prior to the fix,
   * binding a BigDecimal to a DECIMAL column caused the server to SIGABRT (Arrow
   * ValidateDecimalPrecision check failure) or return a Decimal64 schema that the Java Arrow JDBC
   * client rejected. Verifies round-trip through INSERT + SELECT.
   */
  @Test
  @Order(200)
  void testDecimalParameterRoundTrip() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement s = conn.createStatement()) {
      s.execute("DROP TABLE IF EXISTS it_dec");
      s.execute("CREATE TABLE it_dec(id INT, amount DECIMAL(10,2))");

      try (PreparedStatement ps = conn.prepareStatement("INSERT INTO it_dec VALUES (?, ?)")) {
        ps.setInt(1, 1);
        ps.setBigDecimal(2, new java.math.BigDecimal("123.45"));
        assertEquals(1, ps.executeUpdate(), "single-row INSERT should report 1 updated row");
      }

      try (ResultSet rs = s.executeQuery("SELECT amount FROM it_dec WHERE id = 1")) {
        assertTrue(rs.next(), "inserted row should be present");
        assertEquals(
            new java.math.BigDecimal("123.45"),
            rs.getBigDecimal(1),
            "decimal value should round-trip exactly");
      }

      s.execute("DROP TABLE it_dec");
    }
  }

  /**
   * Verifies that {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}
   * returns rows with the JDBC-contracted column names, correct ordinal positions for multi-column
   * indexes, and honors the {@code unique} filter. Powered by the server's {@code
   * _gizmosql_system.main.gizmosql_index_info} catalog view (v1.6.0+), with an inline fallback
   * against {@code duckdb_indexes()} on older servers.
   */
  @Test
  @Order(201)
  void testGetIndexInfo() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement s = conn.createStatement()) {
      s.execute("DROP TABLE IF EXISTS it_ix CASCADE");
      s.execute("CREATE TABLE it_ix(a INT, b INT, c VARCHAR)");
      s.execute("CREATE INDEX it_ix_ab ON it_ix(a, b)");
      s.execute("CREATE UNIQUE INDEX it_ix_c ON it_ix(c)");

      DatabaseMetaData md = conn.getMetaData();

      // Unique-only filter should return exactly one row (the c index).
      try (ResultSet rs = md.getIndexInfo(null, null, "it_ix", true, false)) {
        int rows = 0;
        while (rs.next()) {
          rows++;
          assertEquals("it_ix_c", rs.getString("INDEX_NAME"));
          assertEquals("c", rs.getString("COLUMN_NAME"));
          assertFalse(rs.getBoolean("NON_UNIQUE"), "unique index should have NON_UNIQUE=false");
          assertEquals(1, rs.getShort("ORDINAL_POSITION"));
        }
        assertEquals(1, rows, "unique filter should return one row");
      }

      // Non-unique (all indexes) — two rows for the compound (a,b) index plus one for c.
      try (ResultSet rs = md.getIndexInfo(null, null, "it_ix", false, false)) {
        int ixAbRows = 0;
        int ixCRows = 0;
        while (rs.next()) {
          String name = rs.getString("INDEX_NAME");
          short ord = rs.getShort("ORDINAL_POSITION");
          String col = rs.getString("COLUMN_NAME");
          if ("it_ix_ab".equals(name)) {
            ixAbRows++;
            // Expect a at ord 1, b at ord 2.
            assertEquals(ord == 1 ? "a" : "b", col, "ordinal-position column mapping");
            assertTrue(rs.getBoolean("NON_UNIQUE"));
          } else if ("it_ix_c".equals(name)) {
            ixCRows++;
            assertEquals("c", col);
            assertFalse(rs.getBoolean("NON_UNIQUE"));
          }
        }
        assertEquals(2, ixAbRows, "compound index should yield two rows");
        assertEquals(1, ixCRows, "single-column index should yield one row");
      }

      s.execute("DROP TABLE it_ix CASCADE");
    }
  }

  /**
   * Verifies the non-standard {@code ArrowFlightConnection.getViewDefinition(...)} method returns
   * CREATE VIEW DDL text for an existing view and {@code null} for a missing one. Unwrap via the
   * JDBC {@link Connection#unwrap(Class)} contract.
   */
  @Test
  @Order(202)
  void testGetViewDefinition() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement s = conn.createStatement()) {
      s.execute("DROP TABLE IF EXISTS it_vw_t CASCADE");
      s.execute("CREATE TABLE it_vw_t(x INT)");
      s.execute("CREATE OR REPLACE VIEW it_vw_v AS SELECT x + 1 AS x1 FROM it_vw_t");

      org.apache.arrow.driver.jdbc.ArrowFlightConnection afc =
          conn.unwrap(org.apache.arrow.driver.jdbc.ArrowFlightConnection.class);

      String ddl = afc.getViewDefinition(null, null, "it_vw_v");
      assertNotNull(ddl, "existing view should return DDL");
      assertTrue(
          ddl.toUpperCase(java.util.Locale.ROOT).contains("CREATE"),
          "DDL should start with CREATE");
      assertTrue(ddl.contains("it_vw_v"), "DDL should contain the view name");

      String missing = afc.getViewDefinition(null, null, "it_vw_nonexistent");
      // null is the contract for "view not found".
      // (We intentionally don't assert !=null here — only exact equality with null.)
      assertEquals(null, missing, "missing view should return null");

      s.execute("DROP VIEW it_vw_v");
      s.execute("DROP TABLE it_vw_t");
    }
  }

  /**
   * Verifies that DML statements routed via {@code Statement.execute(...)} produce a valid {@code
   * getUpdateCount()}. Prior to v1.6.0 the Flight SQL JDBC driver treated DML as a query (because
   * the server reported a non-empty result-set schema) and left update count at {@code -1} — hiding
   * the statistics tab in downstream tools like DBeaver.
   */
  @Test
  @Order(203)
  void testDmlUpdateCount() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement s = conn.createStatement()) {
      s.execute("DROP TABLE IF EXISTS it_upd");
      s.execute("CREATE TABLE it_upd(id INT, v VARCHAR)");

      boolean hasRsInsert = s.execute("INSERT INTO it_upd VALUES (1,'a'),(2,'b'),(3,'c')");
      assertFalse(hasRsInsert, "INSERT should not report a result set");
      assertEquals(3, s.getUpdateCount(), "INSERT affected row count");

      boolean hasRsUpdate = s.execute("UPDATE it_upd SET v = 'x' WHERE id < 3");
      assertFalse(hasRsUpdate, "UPDATE should not report a result set");
      assertEquals(2, s.getUpdateCount(), "UPDATE affected row count");

      boolean hasRsDelete = s.execute("DELETE FROM it_upd WHERE id = 1");
      assertFalse(hasRsDelete, "DELETE should not report a result set");
      assertEquals(1, s.getUpdateCount(), "DELETE affected row count");

      // Queries must still go through the query path.
      boolean hasRsSelect = s.execute("SELECT count(*) FROM it_upd");
      assertTrue(hasRsSelect, "SELECT should report a result set");

      s.execute("DROP TABLE it_upd");
    }
  }

  /**
   * Verifies that {@code DatabaseMetaData.getColumns()} surfaces per-column metadata the server now
   * packs into the Arrow Field metadata map (GizmoSQL v1.22.1): real NOT NULL, column comments
   * (REMARKS), auto-increment detection from a {@code nextval(...)} default (IS_AUTOINCREMENT), and
   * the default expression (COLUMN_DEF). Prior to v1.22.1 the server derived the table schema from
   * {@code SELECT * FROM t LIMIT 0}, which marks every field nullable and carries none of this
   * information.
   */
  @Test
  @Order(204)
  void testGetColumnsEnrichment() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement s = conn.createStatement()) {
      s.execute("DROP TABLE IF EXISTS it_meta CASCADE");
      s.execute("DROP SEQUENCE IF EXISTS it_meta_seq");
      s.execute("CREATE SEQUENCE it_meta_seq START 1");
      s.execute(
          "CREATE TABLE it_meta("
              + "  id INT PRIMARY KEY DEFAULT nextval('it_meta_seq'),"
              + "  name VARCHAR NOT NULL,"
              + "  salary DECIMAL(10,2) DEFAULT 50000.00,"
              + "  note VARCHAR"
              + ")");
      s.execute("COMMENT ON COLUMN it_meta.name IS 'employee name'");

      DatabaseMetaData md = conn.getMetaData();
      java.util.Map<String, java.util.Map<String, String>> rows = new java.util.HashMap<>();
      try (ResultSet rs = md.getColumns(null, null, "it_meta", null)) {
        while (rs.next()) {
          java.util.Map<String, String> cols = new java.util.HashMap<>();
          cols.put("IS_NULLABLE", rs.getString("IS_NULLABLE"));
          cols.put("COLUMN_DEF", rs.getString("COLUMN_DEF"));
          cols.put("IS_AUTOINCREMENT", rs.getString("IS_AUTOINCREMENT"));
          cols.put("REMARKS", rs.getString("REMARKS"));
          rows.put(rs.getString("COLUMN_NAME"), cols);
        }
      }

      assertEquals("NO", rows.get("id").get("IS_NULLABLE"));
      assertEquals("NO", rows.get("name").get("IS_NULLABLE"));
      assertEquals("YES", rows.get("salary").get("IS_NULLABLE"));
      assertEquals("YES", rows.get("note").get("IS_NULLABLE"));

      assertEquals("employee name", rows.get("name").get("REMARKS"));

      assertEquals("YES", rows.get("id").get("IS_AUTOINCREMENT"));
      assertEquals("NO", rows.get("name").get("IS_AUTOINCREMENT"));
      assertEquals("NO", rows.get("salary").get("IS_AUTOINCREMENT"));

      String idDefault = rows.get("id").get("COLUMN_DEF");
      assertNotNull(idDefault, "id should expose its default expression");
      assertTrue(
          idDefault.toLowerCase(java.util.Locale.ROOT).contains("nextval"),
          "id default should reference nextval: " + idDefault);
      String salaryDefault = rows.get("salary").get("COLUMN_DEF");
      assertNotNull(salaryDefault);
      assertTrue(salaryDefault.contains("50000"), "salary default mismatch: " + salaryDefault);
      assertEquals(null, rows.get("name").get("COLUMN_DEF"));
      assertEquals(null, rows.get("note").get("COLUMN_DEF"));

      s.execute("DROP TABLE it_meta CASCADE");
      s.execute("DROP SEQUENCE it_meta_seq");
    }
  }

  /**
   * Regression test for the DECIMAL parameter-binding coercion in {@code
   * DecimalAvaticaParameterConverter}. Previously the converter required a {@code BigDecimal} input
   * and threw {@code ClassCastException} (surfaced as "Binding value of type DOUBLE is not yet
   * supported for expected Arrow type Decimal(...)") when the caller passed any other numeric type
   * — which DBeaver's Data Editor does when the user types an un-typed literal like {@code 3} into
   * a DECIMAL column. The fix coerces Double/Integer/Long/String to BigDecimal and rescales to the
   * column's declared scale.
   */
  @Test
  @Order(205)
  void testDecimalBindWithNonBigDecimalInput() throws SQLException {
    assumeServerAvailable();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        Statement s = conn.createStatement()) {
      s.execute("DROP TABLE IF EXISTS it_decbind");
      s.execute("CREATE TABLE it_decbind(id INT, amount DECIMAL(10,3))");

      try (PreparedStatement ps = conn.prepareStatement("INSERT INTO it_decbind VALUES (?, ?)")) {
        // setDouble on a DECIMAL column used to throw; now must coerce cleanly.
        ps.setInt(1, 1);
        ps.setDouble(2, 3.0);
        assertEquals(1, ps.executeUpdate());

        // setObject with a String that parses as a decimal.
        ps.setInt(1, 2);
        ps.setObject(2, "12.345");
        assertEquals(1, ps.executeUpdate());

        // User-entered extra scale should round (HALF_UP) to match the column.
        ps.setInt(1, 3);
        ps.setBigDecimal(2, new java.math.BigDecimal("9.87654"));
        assertEquals(1, ps.executeUpdate());
      }

      try (ResultSet rs = s.executeQuery("SELECT id, amount FROM it_decbind ORDER BY id")) {
        assertTrue(rs.next());
        assertEquals(new java.math.BigDecimal("3.000"), rs.getBigDecimal(2));
        assertTrue(rs.next());
        assertEquals(new java.math.BigDecimal("12.345"), rs.getBigDecimal(2));
        assertTrue(rs.next());
        assertEquals(
            new java.math.BigDecimal("9.877"),
            rs.getBigDecimal(2),
            "9.87654 should round HALF_UP to 9.877 at scale=3");
      }

      s.execute("DROP TABLE it_decbind");
    }
  }
}
