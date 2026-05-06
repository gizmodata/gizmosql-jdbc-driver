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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ArrowFlightMetaImpl#isNonQueryStatement(String)}. This heuristic drives
 * whether {@code Statement.execute(...)} goes through {@code DoPutCommandStatementUpdate} (which
 * produces a valid {@code getUpdateCount()}) or the default prepare-and-query path. Tests cover the
 * DML/DDL keywords, query-shaped keywords, comment stripping, and case sensitivity.
 *
 * <p>This logic is temporary — it will be replaced once the Arrow Flight SQL spec adds an
 * authoritative {@code is_update} field on {@code ActionCreatePreparedStatementResult} (see
 * https://github.com/apache/arrow/pull/49498). See the TODO on {@code isNonQueryStatement} for
 * details.
 */
public class ArrowFlightMetaImplTest {

  @Test
  void dmlKeywordsAreDetected() {
    for (String k :
        new String[] {
          "INSERT INTO t VALUES (1)",
          "UPDATE t SET a = 1",
          "DELETE FROM t",
          "MERGE INTO t USING s ON s.id = t.id",
          "UPSERT INTO t VALUES (1)",
          "REPLACE INTO t VALUES (1)",
          "TRUNCATE TABLE t"
        }) {
      assertTrue(ArrowFlightMetaImpl.isNonQueryStatement(k), () -> "DML should match: " + k);
    }
  }

  @Test
  void ddlKeywordsAreDetected() {
    for (String k :
        new String[] {
          "CREATE TABLE t (a INT)",
          "DROP TABLE t",
          "ALTER TABLE t ADD COLUMN b INT",
          "ATTACH ':memory:' AS m",
          "DETACH m",
          "COPY t FROM 's3://bucket/key'",
          "GRANT SELECT ON t TO alice",
          "REVOKE SELECT ON t FROM alice",
          "COMMENT ON TABLE t IS 'hi'",
          "VACUUM",
          "ANALYZE t",
          "CHECKPOINT",
          "SET memory_limit = '2GB'",
          "RESET memory_limit",
          "USE my_db",
          "BEGIN",
          "COMMIT",
          "ROLLBACK",
          "INSTALL httpfs",
          "LOAD httpfs",
          "EXPORT DATABASE 'path'",
          "IMPORT DATABASE 'path'",
          "CALL my_procedure()"
        }) {
      assertTrue(ArrowFlightMetaImpl.isNonQueryStatement(k), () -> "DDL should match: " + k);
    }
  }

  @Test
  void queryKeywordsAreNotDetected() {
    for (String k :
        new String[] {
          "SELECT 1",
          "WITH t AS (SELECT 1) SELECT * FROM t",
          "VALUES (1), (2)",
          "SHOW TABLES",
          "PRAGMA database_list",
          "EXPLAIN SELECT 1",
          "DESCRIBE t",
          "DESC t"
        }) {
      assertFalse(ArrowFlightMetaImpl.isNonQueryStatement(k), () -> "query shouldn't match: " + k);
    }
  }

  @Test
  void caseInsensitive() {
    assertTrue(ArrowFlightMetaImpl.isNonQueryStatement("insert into t values (1)"));
    assertTrue(ArrowFlightMetaImpl.isNonQueryStatement("Insert Into T Values (1)"));
    assertTrue(ArrowFlightMetaImpl.isNonQueryStatement("INSERT INTO t VALUES (1)"));
  }

  @Test
  void leadingWhitespaceSkipped() {
    assertTrue(ArrowFlightMetaImpl.isNonQueryStatement("   \n\t  INSERT INTO t VALUES (1)"));
  }

  @Test
  void lineCommentsStripped() {
    assertTrue(
        ArrowFlightMetaImpl.isNonQueryStatement("-- preamble\nINSERT INTO t VALUES (1)"),
        "-- line comment then DML");
    assertTrue(
        ArrowFlightMetaImpl.isNonQueryStatement("-- a\n-- b\n  UPDATE t SET a = 1"),
        "multiple line comments");
  }

  @Test
  void blockCommentsStripped() {
    // dbt-style query prefix is a common case worth covering.
    assertTrue(
        ArrowFlightMetaImpl.isNonQueryStatement(
            "/* {\"app\": \"dbt\", \"dbt_version\": \"1.9.0\"} */\nINSERT INTO t VALUES (1)"),
        "dbt block-comment prefix");
    assertTrue(
        ArrowFlightMetaImpl.isNonQueryStatement(
            "/* multi\nline\ncomment */ CREATE TABLE t (a INT)"),
        "multi-line block comment");
  }

  @Test
  void nullOrEmptyReturnsFalse() {
    assertFalse(ArrowFlightMetaImpl.isNonQueryStatement(null));
    assertFalse(ArrowFlightMetaImpl.isNonQueryStatement(""));
    assertFalse(ArrowFlightMetaImpl.isNonQueryStatement("   "));
    assertFalse(ArrowFlightMetaImpl.isNonQueryStatement("-- only a comment\n"));
  }

  @Test
  void unknownKeywordReturnsFalse() {
    assertFalse(ArrowFlightMetaImpl.isNonQueryStatement("FNORD something"));
  }

  /**
   * Documented known limitation of this heuristic: a CTE that wraps a DML statement starts with
   * {@code WITH} and therefore won't be routed through the update path. This is the kind of case
   * the upstream Arrow {@code is_update} protocol field would handle authoritatively. Kept here as
   * a pinned-down expectation so that if we ever extend the heuristic, we make this choice
   * deliberately.
   */
  @Test
  void cteWrappedDmlIsMissedOnPurpose() {
    assertFalse(
        ArrowFlightMetaImpl.isNonQueryStatement(
            "WITH new_rows AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM new_rows"),
        "heuristic classifies by leading keyword; WITH-wrapped DML is a known false negative"
            + " that will be resolved by adopting apache/arrow#49498");
    // Sanity check: the equivalence assertion is the useful one — if someone switches on a
    // CTE-aware parser the assertion will flip and this test can be deleted.
    assertEquals(
        false,
        ArrowFlightMetaImpl.isNonQueryStatement(
            "WITH s AS (SELECT 1) INSERT INTO t SELECT * FROM s"));
  }

  // ---------------------------------------------------------------------------
  // RETURNING carve-out — DML that produces a result set must NOT be classified
  // as non-query, since the executeUpdate (DoPut) path drops the returned rows
  // and Statement.executeQuery() then raises "Statement did not return a result
  // set". See {@link ArrowFlightMetaImpl#hasReturningClause}.
  // ---------------------------------------------------------------------------

  @Test
  void dmlWithReturningFallsThroughToQueryPath() {
    for (String sql :
        new String[] {
          "INSERT INTO t (name) VALUES ('a') RETURNING id",
          "INSERT INTO t (name) VALUES ('a') RETURNING id, name",
          "UPDATE t SET x = 1 WHERE id = 1 RETURNING *",
          "DELETE FROM t WHERE id = 1 RETURNING id",
          "MERGE INTO t USING s ON s.id = t.id WHEN MATCHED THEN UPDATE SET x = 1 RETURNING *",
          // mixed-case keyword
          "insert into t values ('a') returning id",
          "Insert Into t Values ('a') Returning id",
          // newline before RETURNING
          "INSERT INTO t VALUES ('a')\nRETURNING id",
          // comment-prefixed (dbt-style query tag) plus RETURNING
          "/* tag */ INSERT INTO t VALUES (1) RETURNING id",
          "-- log\nUPDATE t SET x = 1 RETURNING x",
          // trailing semicolon
          "INSERT INTO t VALUES ('a') RETURNING id;"
        }) {
      assertFalse(
          ArrowFlightMetaImpl.isNonQueryStatement(sql),
          () -> "DML with RETURNING must not be classified as non-query: " + sql);
    }
  }

  @Test
  void returningInsideStringLiteralIsNotMistakenForClause() {
    // The literal value is the word 'returning' — must still route to
    // executeUpdate, since there's no actual RETURNING clause.
    assertTrue(
        ArrowFlightMetaImpl.isNonQueryStatement("INSERT INTO t (msg) VALUES ('returning')"),
        "RETURNING inside a single-quoted string literal must not trigger the carve-out");
    // Escaped inner quote ('') must not break parsing — apostrophe in the value.
    assertTrue(
        ArrowFlightMetaImpl.isNonQueryStatement("INSERT INTO t (msg) VALUES ('it''s returning')"),
        "doubled single quote inside a literal must not break literal-stripping");
    // After the literal ends, an actual RETURNING clause must be detected.
    assertFalse(
        ArrowFlightMetaImpl.isNonQueryStatement(
            "INSERT INTO t (msg) VALUES ('returning') RETURNING id"),
        "RETURNING outside the literal must still trigger the carve-out");
  }

  @Test
  void returningInsideDoubleQuotedIdentifierIsNotMistakenForClause() {
    // A column literally named "returning" — DuckDB-legal with quoting.
    assertTrue(
        ArrowFlightMetaImpl.isNonQueryStatement("INSERT INTO t (\"returning\") VALUES (1)"),
        "RETURNING inside a double-quoted identifier must not trigger the carve-out");
  }

  @Test
  void hasReturningClauseDirect() {
    // Direct sanity checks on the helper that backs the carve-out.
    assertTrue(ArrowFlightMetaImpl.hasReturningClause("INSERT INTO t VALUES (1) RETURNING id"));
    assertTrue(ArrowFlightMetaImpl.hasReturningClause("returning id"));
    assertFalse(ArrowFlightMetaImpl.hasReturningClause("INSERT INTO t VALUES (1)"));
    assertFalse(ArrowFlightMetaImpl.hasReturningClause("SELECT * FROM t"));
    assertFalse(ArrowFlightMetaImpl.hasReturningClause(null));
    assertFalse(ArrowFlightMetaImpl.hasReturningClause(""));
    // Substring of a longer word must NOT match — \b word-boundary check.
    assertFalse(ArrowFlightMetaImpl.hasReturningClause("SELECT returningish FROM t"));
    assertFalse(ArrowFlightMetaImpl.hasReturningClause("SELECT prereturning FROM t"));
  }
}
