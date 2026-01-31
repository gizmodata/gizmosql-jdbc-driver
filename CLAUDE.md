# Claude Code Notes - GizmoSQL JDBC Driver

## Project Overview
Fork of Apache Arrow Java, producing a shaded JDBC driver JAR (`com.gizmodata:gizmosql-jdbc-driver`) published to Maven Central.

## Build
- **Build with Java 11**: `JAVA_HOME=$(/usr/libexec/java_home -v 11) ./mvnw -B -pl flight/flight-sql-jdbc-driver -am -DskipTests package`
- Use `./mvnw` (Maven wrapper), not bare `mvn`
- The shaded JAR is at `flight/flight-sql-jdbc-driver/target/gizmosql-jdbc-driver-VERSION.jar`
- To install to local Maven repo: replace `package` with `install`
- To skip formatting/linting locally: add `-Dspotless.check.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true`

## Important: Spotless Formatting
- The project uses `spotless-maven-plugin` with Google Java Format
- **Always run `./mvnw clean spotless:apply -pl <module>` before committing** to avoid CI failures
- Use `clean` to bust the spotless cache — without it, cached results may hide violations
- Or run `./mvnw clean spotless:check -pl <module>` to verify
- Google Java Format rules to watch for:
  - Javadoc: second paragraph in a `/** */` block needs `<p>` tag (e.g., ` * <p>Second paragraph...`)
  - Standard Google Java style: 2-space indent, specific import ordering, etc.
- Spotless is incompatible with JDK 25+ (google-java-format uses internal JDK APIs) — CI skips it on JDK 25

## Module Structure (relevant modules)
- `memory/memory-netty-buffer-patch` — Custom Netty buffer wrappers (`UnsafeDirectLittleEndian`, `PooledByteBufAllocatorL`)
- `memory/memory-netty` — `NettyAllocationManager`, `DefaultAllocationManagerFactory`
- `memory/memory-core` — `BaseAllocator`, `RootAllocator`, `AllocationManager`
- `flight/flight-sql-jdbc-core` — JDBC driver implementation
- `flight/flight-sql-jdbc-driver` — Shaded uber JAR (groupId: `com.gizmodata`, artifactId: `gizmosql-jdbc-driver`)

## Netty Buffer Patch (Java 25+ Compatibility)
- `UnsafeDirectLittleEndian` extends `MutableWrappedByteBuf` (NOT `WrappedByteBuf`)
  - Netty 4.2.x made `memoryAddress()` and other methods `final` in `WrappedByteBuf`
  - `MutableWrappedByteBuf` allows overriding these methods
- Constructor catches `UnsupportedOperationException` from `buf.memoryAddress()` for `EmptyByteBuf`
- Overrides `memoryAddress()` to return cached address instead of delegating to wrapped buffer
- `PooledByteBufAllocatorL.InnerAllocator` uses reflection on `PooledByteBufAllocator.directArenas` field

## Release Process
1. Commit and push to `main`
2. Tag with `v<version>` (e.g., `v1.2.0`)
3. CI builds, tests (JDK 11/17/21/25), then publishes to Maven Central
4. Workflow uses `versions:set` to set version from tag, pins `arrow-bom` to `19.0.0-SNAPSHOT`
5. GPG signing with `GPG_PRIVATE_KEY` secret — public key must be on `keyserver.ubuntu.com`
6. Maven Central credentials: `MAVEN_USERNAME` / `MAVEN_PASSWORD` secrets

## CI (`.github/workflows/jdbc-driver.yml`)
- Build & unit tests: JDK 11, 17, 21, 25
- Integration tests: JDK 11, 17, 21, 25 (against `gizmodata/gizmosql:latest` container)
- Concurrency group cancels in-progress runs on same ref
- Tag pushes trigger Maven Central publish + GitHub release

## Common Gotchas
- Rebuilding only `memory-netty-buffer-patch` is NOT enough — must rebuild the full shaded driver with `-am`
- The shaded JAR relocates all classes under `org.apache.arrow.driver.jdbc.shaded.*`
- JAR timestamps inside shaded JARs show original compile time, not rebuild time — don't trust them
- Maven Central rejects duplicate version uploads — if publish fails and you re-tag, it will fail again
- `concurrency: cancel-in-progress: true` means rapid pushes cancel earlier runs — be careful with tag + main pushes close together
