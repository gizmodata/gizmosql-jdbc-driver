# Changelog

All notable changes to the GizmoSQL JDBC Driver will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.5.3] - 2026-04-11

### Fixed
- **Duplicated columns in result set metadata for empty results.** When a query returned zero rows (e.g. `SELECT * FROM empty_table`), `ResultSetMetaData` exposed every column twice. As soon as the table had data, columns appeared correctly; deleting all rows reproduced the bug. Root cause: `ArrowFlightStatement.executeFlightInfoQuery()` called `signature.columns.addAll(...)` on a list that `ArrowFlightMetaImpl.prepareForHandle()` had already populated, doubling the entries. Normally `ArrowFlightJdbcVectorSchemaRootResultSet.populateData()` clears and repopulates the list before the cursor is built — but that path is gated on `currentEndpointData != null`, and `FlightEndpointDataQueue.enqueue()` returns `null` from its future when no batch has `rowCount > 0`. An empty result therefore skipped the clear, leaving the doubled metadata visible to callers. Fix: clear `signature.columns` before adding in `executeFlightInfoQuery()`.

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
