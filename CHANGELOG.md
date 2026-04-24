# Changelog

All notable changes to the GizmoSQL JDBC Driver will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.6.1] - 2026-04-24

### Fixed
- **DECIMAL parameter binding no longer rejects non-`BigDecimal` values.** When DBeaver's Data Editor (and other Avatica-backed tools) passes an un-typed numeric literal like `3` for a DECIMAL column, Avatica presents it as a Double/Integer/Long. The previous `DecimalAvaticaParameterConverter` did a naked cast `(BigDecimal) typedValue.toLocal()` and threw `ClassCastException`, surfacing as "Binding value of type DOUBLE is not yet supported for expected Arrow type Decimal(p, s, 128)". The converter now coerces `Double` / `Integer` / `Long` / `String` to `BigDecimal` via `toString()` (avoiding the well-known `new BigDecimal(double)` precision pitfall), rescales to the column's declared scale (lossless widen, HALF_UP rounding on truncation — matches the behavior every other JDBC driver gives), and binds cleanly.

### Added
- **`DatabaseMetaData.getColumns()` now populates `COLUMN_DEF`.** GizmoSQL server v1.22.1 publishes the column default expression in a vendor-prefixed Arrow Field metadata key (`GIZMOSQL:COLUMN_DEFAULT`) — there is no upstream Apache Arrow Flight SQL spec key for COLUMN_DEF. `ArrowDatabaseMetadata.getColumns()` now reads that key and fills the JDBC `COLUMN_DEF` column when present, so DBeaver (and other JDBC clients) can display "Default" values in their column browser and Data Editor. The server's existing `ARROW:FLIGHT:SQL:REMARKS` and `ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT` metadata keys are already read by the stock Arrow Flight SQL JDBC driver — they produce correct `REMARKS` and `IS_AUTOINCREMENT` values once the server fills them (also shipping in v1.22.1).

### Testing
- New integration tests in `GizmoSqlIntegrationIT`:
  - `testGetColumnsEnrichment` — verifies `NULLABLE`, `REMARKS`, `IS_AUTOINCREMENT`, and `COLUMN_DEF` are all populated correctly for a table with NOT NULL, column comments, a `nextval(...)` default, and a literal default.
  - `testDecimalBindWithNonBigDecimalInput` — pins the DECIMAL coercion path (`setDouble`, `setObject(String)`, `setBigDecimal` with extra scale), including the HALF_UP rounding expectation.

## [1.6.0] - 2026-04-24

### Added
- **`DatabaseMetaData.getIndexInfo()` now returns actual index metadata (DuckDB backend).** Flight SQL has no `GetIndexInfo` RPC, so upstream Arrow Flight SQL JDBC returns an empty result set. The GizmoSQL driver now queries the server's `_gizmosql_system.main.gizmosql_index_info` catalog view (introduced in GizmoSQL server v1.22.0) to return JDBC-contracted rows: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `NON_UNIQUE`, `INDEX_QUALIFIER`, `INDEX_NAME`, `TYPE`, `ORDINAL_POSITION`, `COLUMN_NAME`, `ASC_OR_DESC`, `CARDINALITY`, `PAGES`, `FILTER_CONDITION`. Backward compatible with older GizmoSQL servers: if the catalog view doesn't exist, falls back to an inline query against `duckdb_indexes()` that emulates the same schema. If neither is available (SQLite backend), raises a clear `SQLException` with an upgrade/backend hint rather than silently returning empty. Unlocks index visibility in downstream clients like DBeaver.
- **New `ArrowFlightConnection.getViewDefinition(catalog, schema, view)` method** returns CREATE VIEW DDL text for a named view. JDBC has no standard API for view source, so this is a non-standard extension reachable via `Connection.unwrap(ArrowFlightConnection.class).getViewDefinition(...)`. Backed by the server's `_gizmosql_system.main.gizmosql_view_definition` catalog view, with an inline `duckdb_views()` fallback for older servers and a clear "SQLite backend not supported" error when neither is available. Returns `null` when the view is not found.
- **Statement-level DML now reports accurate `getUpdateCount()`** for INSERT / UPDATE / DELETE / MERGE / DDL executed through `Statement.execute(sql)`. Previously the Flight SQL JDBC driver treated every statement as a query-producing prepared statement; DuckDB reports a `Count`-shaped result schema for DML, so `StatementType` resolved to `SELECT`, `getUpdateCount()` stayed at `-1`, and tools like DBeaver couldn't surface execution statistics. `ArrowFlightMetaImpl.prepareAndExecute(...)` now sniffs the leading keyword (with SQL comment stripping, case insensitive, Locale-safe) and routes DML through `DoPutCommandStatementUpdate`, returning a valid update count. Keyword set is deliberately kept in sync with the ADBC Python driver's `_DDL_DML_KEYWORDS` so both drivers classify statements the same way. A TODO in source code points at the upstream Apache Arrow proposal [arrow#49498](https://github.com/apache/arrow/pull/49498) to replace this heuristic with an authoritative `is_update` protocol field — one known false negative of the current heuristic is CTE-wrapped DML like `WITH ... INSERT ... RETURNING *`.

### Testing
- New integration tests in `GizmoSqlIntegrationIT` exercise each of the additions above against a live GizmoSQL server: `testDecimalParameterRoundTrip` (regression for the server-side v1.22.0 DECIMAL fix), `testGetIndexInfo`, `testGetViewDefinition`, `testDmlUpdateCount`.
- New unit tests in `ArrowFlightMetaImplTest` pin the `isNonQueryStatement` keyword-classification heuristic (DDL, DML, query-shaped keywords, line/block comments, case insensitivity, dbt-style comment prefixes, CTE-wrapped-DML known miss).
- Upstream test `ArrowDatabaseMetadataTest#testGetIndexInfo` (which asserted stock empty-result behavior) was renamed to `testGetIndexInfoAgainstNonGizmosqlServerThrows` and updated to match the new contract — driver now throws `SQLException` on non-GizmoSQL Flight SQL servers rather than returning an empty result set.

## [1.5.5] - 2026-04-11

### Changed
- **Release infrastructure: unversioned `gizmosql-jdbc-driver.jar` alias on every GitHub Release.** The publish job now also uploads the main shaded jar under a versionless filename, alongside the existing `-VERSION.jar` / `-sources.jar` / `-javadoc.jar` / `-tests.jar` assets. This makes `https://github.com/gizmodata/gizmosql-jdbc-driver/releases/latest/download/gizmosql-jdbc-driver.jar` auto-redirect to the newest release jar — letting the gizmodata-website repo retire its k8s nginx VirtualServer that hand-redirects `downloads.gizmodata.com/gizmosql-jdbc-driver/latest` to a hardcoded Maven Central URL bumped by hand on every release. The unversioned copy is bit-identical to the versioned main jar; canonical artifacts and Maven Central publishing are unchanged. Excluded from build attestation since it is a rename alias of an already-attested artifact.
- **Release infrastructure: CHANGELOG.md sections now auto-populate GitHub Release notes.** A new "Extract changelog section for this tag" step in the publish-release job parses `CHANGELOG.md` for the entry matching the current tag and passes it to `softprops/action-gh-release` as `body`. With `generate_release_notes: true` still set, the curated notes appear above the auto-generated PR/commit list — no more copying CHANGELOG entries into the GitHub Release page by hand after every tag. This entry is the first one created via that pipeline.

## [1.5.4] - 2026-04-11

### Fixed
- **Duplicated columns in result set metadata for empty results.** When a query returned zero rows (e.g. `SELECT * FROM empty_table`), `ResultSetMetaData` exposed every column twice. As soon as the table had data, columns appeared correctly; deleting all rows reproduced the bug. Root cause: `ArrowFlightStatement.executeFlightInfoQuery()` called `signature.columns.addAll(...)` on a list that `ArrowFlightMetaImpl.prepareForHandle()` had already populated, doubling the entries. Normally `ArrowFlightJdbcVectorSchemaRootResultSet.populateData()` clears and repopulates the list before the cursor is built — but that path is gated on `currentEndpointData != null`, and `FlightEndpointDataQueue.enqueue()` returns `null` from its future when no batch has `rowCount > 0`. An empty result therefore skipped the clear, leaving the doubled metadata visible to callers. Fix: clear `signature.columns` before adding in `executeFlightInfoQuery()`. Pinned by a regression test in `ResultSetMetadataTest` that asserts `getColumnCount()` for an all-empty-roots query.
- **Stale `flight-sql-jdbc-core` bytecode in the publish workflow.** v1.5.3 was tagged with the source fix above but shipped to Maven Central with bit-identical bytecode to 1.5.2 (the published `ArrowFlightStatement.class` was 3799 bytes in both). Same class of bug as the v1.3.0–v1.4.0 `memory-netty-buffer-patch` stale-bytecode incident: because `flight-sql-jdbc-driver` declares `flight-sql-jdbc-core` with no explicit version (managed via the `arrow-bom` import which the workflow pins to `19.0.0-SNAPSHOT`), the reactor with `-am` does not pull `flight-sql-jdbc-core` in as a sibling at the renumbered release version, and the shade plugin instead resolves `flight-sql-jdbc-core:19.0.0-SNAPSHOT` from the `actions/setup-java`-restored `~/.m2` cache — which is whatever was there from a previous run. The publish workflow now purges `~/.m2/repository/org/apache/arrow/flight-sql-jdbc-core` and rebuilds it from source as `19.0.0-SNAPSHOT` before `versions:set`, mirroring the existing memory-netty-buffer-patch handling. A new bytecode-verification step also asserts `List.clear` is present in `executeFlightInfoQuery()` of the shaded `ArrowFlightStatement.class` — so any future stale-bytecode regression in this file fails the publish job loudly instead of silently shipping.

## [1.5.2] - 2026-04-10

### Fixed
- **Fail fast with a clear error when credentials are missing or partial.** Previously, building a client with no `user`/`password` (or only one of the two) silently skipped the basic auth handshake, letting `DriverManager.getConnection` return. The first metadata call would then surface the cryptic server-side error `Invalid Authorization Header type!` — seen by users of DBeaver and similar tools when the driver was handed incomplete credentials. `ArrowFlightSqlClientHandler.Builder.build()` now validates credentials up front and throws `SQLException` with actionable messages (e.g. `"No credentials provided. GizmoSQL requires authentication: set 'user' and 'password' connection properties, or 'token', or configure OAuth (authType=external)."`). Token and OAuth paths are unchanged.

### Changed
- Removed upstream Apache Arrow tests that exercised anonymous/no-auth Flight SQL connections (`ConnectionTest#testGetBasicClientNoAuth*`, `ConnectionTlsTest#testGetNonAuthenticatedEncrypted*`, `ConnectionMutualTlsTest#testGetNonAuthenticatedEncrypted*`, `ConnectionTlsRootCertsTest#testGetNonAuthenticatedEncrypted*`). GizmoSQL requires authentication on every connection, so these scenarios are no longer supported; the new behavior is pinned by `ArrowFlightSqlClientHandlerBuilderTest#testBuildRejects*` (5 regression tests).

## [1.5.1] - 2026-03-10

### Fixed
- Always send `CloseSession` RPC when closing a JDBC connection. Previously, `CloseSession` was only sent if a catalog had been explicitly set, leaving server-side sessions open for most connections.
- Suppress `UNIMPLEMENTED` and `UNAUTHENTICATED` errors from `CloseSession` during connection close, matching existing handling of `UNAVAILABLE`.
- Enable TLS unit tests in CI by setting `ARROW_TEST_DATA` and regenerating JKS keystores from the arrow-testing submodule.

### Changed
- Port upstream [GH-1007](https://github.com/apache/arrow-java/issues/1007): `MemoryUtil` no longer crashes during class loading if `--add-opens` is not set; instead degrades gracefully.
- Port upstream [GH-130](https://github.com/apache/arrow-java/issues/130): `AutoCloseables` null-safety — prevents `NullPointerException` in resource cleanup paths.
- Port upstream [GH-1038](https://github.com/apache/arrow-java/issues/1038): Reduce per-instance memory overhead in `ArrowBuf`, `BufferLedger`, and `Accountant` by replacing `AtomicLong`/`AtomicInteger` fields with static field updaters.

## [1.5.0] - 2026-02-11

### Added
- **Server-side OAuth / SSO support:** Browser-based Single Sign-On for desktop tools like DBeaver and IntelliJ. Set `authType=external` and `oauthServerPort=<port>` — the GizmoSQL server handles the entire OAuth code exchange as a confidential client. No client IDs, secrets, or OAuth configuration needed on the JDBC side.
- **OAuth URL discovery handshake:** The driver automatically discovers the OAuth server URL (including correct HTTP/HTTPS scheme) from the GizmoSQL server, so it works correctly even when the OAuth port uses different TLS settings than the main Flight port.

### Changed
- User-Agent string now reports `GizmoSQL JDBC Driver <version>` instead of `JDBC Flight SQL Driver <version>`.
- Simplified OAuth connection properties: only `authType=external` and `oauthServerPort` are needed (removed client-side OIDC/PKCE properties).

### Removed
- Client-side OAuth flows (Authorization Code + PKCE, Client Credentials, Token Exchange) — replaced by server-side OAuth which is simpler and more secure.

### Fixed
- Improved TLS error messages: connection failures due to TLS misconfiguration now include actionable guidance (e.g., suggesting `useEncryption=true` or `disableCertificateVerification=true`).

## [1.4.1] - 2026-01-22

### Fixed
- Fix stale bytecode in shaded JAR for `memory-netty-buffer-patch` module (v1.3.0 through v1.4.0 were affected).

## [1.4.0] - 2026-01-21

### Fixed
- JDK 25 compatibility: use `MutableWrappedByteBuf` for Netty 4.2.x final method changes.

## [1.3.2] - 2026-01-20

### Fixed
- CI: remove Develocity extension and purge cached artifacts to prevent stale bytecode.

## [1.3.1] - 2026-01-20

### Fixed
- CI: disable Develocity build cache that was persisting stale compiled classes.

## [1.3.0] - 2026-01-19

### Added
- JDK 25 compatibility for Netty buffer operations.
- CI: build and test on JDK 11, 17, 21, and 25.

## [1.1.0] - 2025-12-15

### Added
- Initial fork from Apache Arrow Java with GizmoSQL JDBC driver packaging.
- Shaded uber JAR published to Maven Central as `com.gizmodata:gizmosql-jdbc-driver`.

## [1.0.0] - 2025-11-01

### Added
- Initial release of GizmoSQL JDBC Driver.
