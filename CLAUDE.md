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

## JDK 25+ Compatibility Notes
- Netty 4.2.x sets `io.netty.noUnsafe=true` by default on JDK 25+ — must pass `-Dio.netty.noUnsafe=false`
- JDK 25 requires `--sun-misc-unsafe-memory-access=allow` for sun.misc.Unsafe memory methods
- JDK 16+ requires `--enable-native-access=ALL-UNNAMED` for native memory access
- These are handled by Maven profiles `jdk16-native-access` and `jdk25-unsafe-access` in root pom.xml
- Mockito/ByteBuddy cannot mock classes on JDK 25 — `flight-sql-jdbc-core` unit tests are skipped on JDK 25 (integration tests provide coverage)
- H2 database (used by `arrow-jdbc` tests) is incompatible with JDK 25
- XML comments in pom.xml must not contain `--` (double dashes) — Maven's XML parser rejects them

## Local Testing with act
- Use `act push -j build --matrix jdk:25 --detect-event` to test CI locally before pushing
- TLS connection tests (`ConnectionTlsTest`, etc.) fail in act/Docker due to missing cert files — these pass on GitHub CI
- Always validate with act before pushing to avoid burning GitHub Actions minutes

## Release Checklist
- Update version references in `README.md` (Maven, Gradle, badge) before tagging
- Update `gizmosqlline` pom.xml and README with new driver version after Maven Central publish

## Current Work: JDK 25 Bytecode Fix (Jan 31 2026)

### Problem
`PooledByteBufAllocatorL.java` was fixed to use `AbstractByteBuf` instead of `PooledUnsafeDirectByteBuf`, but published JARs (v1.3.0 through v1.4.0) all had stale bytecode.

### Root Cause (CONFIRMED)
`memory-netty-buffer-patch` is NOT in the `-am` reactor for `flight-sql-jdbc-driver` because it is a **shade-time dependency**, not a Maven dependency. The shade plugin pulls it from `~/.m2/repository`. The publish-release job never compiled it from source.

### Fix Applied (committed to main, not yet successfully published)
In `.github/workflows/jdbc-driver.yml` publish-release job: build `memory-netty-buffer-patch` as a **separate Maven invocation BEFORE** `versions:set`, so the freshly compiled `19.0.0-SNAPSHOT` jar gets installed to `~/.m2/repository`. Then the shade plugin picks up the correct bytecode.

### Local Verification Status
- Source code is correct: `PooledByteBufAllocatorL.java` uses `AbstractByteBuf` everywhere
- Unshaded compiled class (in `memory-netty-buffer-patch/target/classes/`) has `AbstractByteBuf` references, zero `PooledUnsafeDirectByteBuf` references
- Local build of shaded driver JAR needs to be tested on JDK 25 against gizmosqlline

### Next Steps
1. Build driver locally: already done, JAR at `flight/flight-sql-jdbc-driver/target/gizmosql-jdbc-driver-19.0.0-SNAPSHOT.jar`
2. Build gizmosqlline using local SNAPSHOT driver (change pom.xml to `19.0.0-SNAPSHOT`)
3. Test gizmosqlline on JDK 25 against local GizmoSQL (already running on port 31337)
4. If it works, tag driver as v1.4.1 (CI fix already pushed to main)
5. Wait for Maven Central sync, then update gizmosqlline and tag

### Tags Published to Maven Central (all have stale bytecode)
- v1.3.0, v1.3.1, v1.3.2, v1.4.0 — all have `PooledUnsafeDirectByteBuf` casts
- v1.4.1 tag was deleted after publish-release failed (version mismatch error)

### Key Insight About Purge Path
The Maven artifactId is `arrow-memory-netty-buffer-patch` (with `arrow-` prefix). The module directory is `memory/memory-netty-buffer-patch`. The purge path must be `~/.m2/repository/org/apache/arrow/arrow-memory-netty-buffer-patch`.

## Common Gotchas
- **Develocity build cache (REMOVED)**: The Develocity Maven extension was removed from `.mvn/extensions.xml` because its local build cache persisted stale compiled classes across GitHub Actions runs (restored via `setup-java` Maven cache). Neither `clean` nor `-Ddevelocity.cache.local.enabled=false` prevented it. v1.3.0 and v1.3.1 were published with stale bytecode as a result. CI now purges `~/.m2/repository/org/apache/arrow/memory-netty-buffer-patch` before builds and has a bytecode verification step.
- Rebuilding only `memory-netty-buffer-patch` is NOT enough — must rebuild the full shaded driver with `-am`
- The shaded JAR relocates all classes under `org.apache.arrow.driver.jdbc.shaded.*`
- JAR timestamps inside shaded JARs show original compile time, not rebuild time — don't trust them
- Maven Central rejects duplicate version uploads — if publish fails and you re-tag, it will fail again
- `concurrency: cancel-in-progress: true` means rapid pushes cancel earlier runs — be careful with tag + main pushes close together
