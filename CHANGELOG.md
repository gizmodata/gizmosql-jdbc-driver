# Changelog

All notable changes to the GizmoSQL JDBC Driver will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.5.0] - 2026-02-09

### Added
- **OAuth / SSO support (OIDC Authorization Code + PKCE):** Browser-based Single Sign-On for desktop tools like DBeaver and IntelliJ. New connection properties: `oidc.issuer`, `oidc.clientId`, `oidc.clientSecret`, `oidc.scopes`.
- OIDC auto-discovery: the driver fetches `.well-known/openid-configuration` to resolve authorization and token endpoints automatically.
- PKCE (Proof Key for Code Exchange) with S256 challenge method for secure public-client flows.
- Temporary localhost callback server to receive the authorization code from the IdP redirect.
- Token caching and automatic refresh using refresh tokens.
- Support for IdPs that require a client secret (e.g., Google Desktop apps) via `oidc.clientSecret`.
- Prefer OIDC ID token (JWT) over opaque access token when both are returned by the IdP.

### Changed
- User-Agent string now reports `GizmoSQL JDBC Driver <version>` instead of `JDBC Flight SQL Driver <version>`.

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
