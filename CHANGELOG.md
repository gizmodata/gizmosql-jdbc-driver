# Changelog

All notable changes to the GizmoSQL JDBC Driver will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.5.1] - 2026-02-21

### Fixed
- Always send `CloseSession` RPC when closing a JDBC connection. Previously, `CloseSession` was only sent if a catalog had been explicitly set, leaving server-side sessions open for most connections.

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
