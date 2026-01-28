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
    boolean useTls = Boolean.parseBoolean(System.getenv().getOrDefault("GIZMOSQL_USE_TLS", "false"));

    String scheme = useTls ? "arrow-flight-sql" : "arrow-flight-sql";
    jdbcUrl = String.format("jdbc:%s://%s:%s", scheme, host, port);

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
        ResultSet rs = stmt.executeQuery("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")) {

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
      stmt.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR, value DOUBLE)");

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
            stmt.executeQuery(
                "SELECT 42 AS int_col, 'hello' AS str_col, 3.14 AS double_col")) {

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
            stmt.executeQuery(
                "SELECT list(i) AS arr FROM (SELECT unnest([1,2,3,4,5]) AS i)")) {

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

}
