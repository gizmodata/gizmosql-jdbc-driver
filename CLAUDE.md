# Claude Code Notes - GizmoSQL JDBC Driver

## Project Overview
Fork of Apache Arrow Java, producing a shaded JDBC driver JAR (`com.gizmodata:gizmosql-jdbc-driver`) published to Maven Central.

## Build
- **Build with Java 17** (minimum since v1.7.0, following upstream GH-1078): `JAVA_HOME=$(/usr/libexec/java_home -v 17) ./mvnw -B -pl flight/flight-sql-jdbc-driver -am -DskipTests package`
- Use `./mvnw` (Maven wrapper), not bare `mvn`
- JDK 21/25 here are Homebrew installs not registered with `java_home`: use `JAVA_HOME=/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home`
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
3. CI builds and runs unit + live e2e tests (JDK 17/21/25) on the tagged commit, then publishes to Maven Central
4. Workflow uses `versions:set` to set version from tag, pins `arrow-bom` to `19.0.0-SNAPSHOT`
5. GPG signing with `GPG_PRIVATE_KEY` secret — public key must be on `keyserver.ubuntu.com`
6. Maven Central credentials: `MAVEN_USERNAME` / `MAVEN_PASSWORD` secrets

## CI (`.github/workflows/jdbc-driver.yml`)
- Build & unit tests: JDK 17, 21, 25
- Integration tests: JDK 17, 21, 25 against both a pinned GizmoSQL release (`v1.40.0`) and `latest`; `docker-compose.test.yml` pins the same release for local runs. Bump both together.
- **Release gate**: `publish-snapshot` and `publish-release` must `needs: [build, integration-test]`. `GizmoSqlIntegrationIT` fails (never skips) when the server is unreachable, and failsafe has `failIfNoTests=true`. Don't reintroduce `assumeTrue`-style skips: an unreachable server would then pass the gate with zero e2e coverage.
- Local e2e run: start a server (e.g. `docker compose -f docker-compose.test.yml up -d`), then `GIZMOSQL_PORT=31337 ./mvnw -pl flight/flight-sql-jdbc-core -Pintegration-tests failsafe:integration-test failsafe:verify`
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

Before tagging a new release:

1. **Update `CHANGELOG.md`** — Add entries under a new version heading (`## [X.Y.Z] - YYYY-MM-DD`). Follow [Keep a Changelog](https://keepachangelog.com/) format. The publish workflow extracts the matching section and prepends it to the GitHub Release notes (above the auto-generated PR/commit list), so this is the canonical release-notes source — no need to copy it into the GitHub Release by hand.
2. **Update `README.md`** — Update version references in Maven/Gradle snippets and the badge URL. **Keep version numbers in sync between `README.md` and `CHANGELOG.md`.**
3. **Run Spotless** — `./mvnw clean spotless:apply -pl flight/flight-sql-jdbc-core` to format all source.
4. **Commit and push to `main`**.
5. **Tag with `v<version>`** (e.g., `git tag v1.5.1 && git push origin v1.5.1`).
6. **After Maven Central publish** — Update `gizmosqlline` pom.xml and README with new driver version.

## Stale Bytecode History (v1.3.0–v1.4.0)
- `PooledByteBufAllocatorL.java` source was fixed to use `AbstractByteBuf` instead of `PooledUnsafeDirectByteBuf`, but v1.3.0 through v1.4.0 all shipped with stale bytecode
- **Root cause**: `memory-netty-buffer-patch` is a shade-time dependency (not a Maven `<dependency>`), so `-am` doesn't rebuild it — the shade plugin pulls it from `~/.m2/repository`
- **Fix** (v1.4.1): CI publish-release job now builds `memory-netty-buffer-patch` as a separate Maven invocation BEFORE `versions:set`, so freshly compiled `19.0.0-SNAPSHOT` classes get installed to `~/.m2/repository`
- The Maven artifactId is `arrow-memory-netty-buffer-patch` (with `arrow-` prefix); the module directory is `memory/memory-netty-buffer-patch` — the purge path must use the artifactId

## Syncing with upstream apache/arrow-java
- The fork has no merge relationship with `upstream`; the merge-base is stuck at 2026-01-27. Upstream fixes come in as `git cherry-pick -x` or hand-ports, so `rev-list` ahead/behind counts are meaningless.
- To triage: list upstream commits since the last sync, then check each with `git show <sha> | git apply --check -R` (succeeds = already present). A "conflict" may be a fix we already shipped in another form: GH-44 was our 6ca415aa3, months earlier.
- Take only fixes that reach the shaded JAR and that GizmoSQL actually exercises. Run OSV (`api.osv.dev/v1/querybatch`) over `dependency:tree -Dscope=runtime` of `flight/flight-sql-jdbc-driver`. Upstream's versions are not always clean: on 2026-10-01 its Jackson 2.22.0 still had advisories.

## Common Gotchas
- **Develocity build cache (REMOVED)**: The Develocity Maven extension was removed from `.mvn/extensions.xml` because its local build cache persisted stale compiled classes across GitHub Actions runs (restored via `setup-java` Maven cache). Neither `clean` nor `-Ddevelocity.cache.local.enabled=false` prevented it. v1.3.0 and v1.3.1 were published with stale bytecode as a result. CI now purges `~/.m2/repository/org/apache/arrow/arrow-memory-netty-buffer-patch` before builds and has a bytecode verification step.
- Rebuilding only `memory-netty-buffer-patch` is NOT enough — must rebuild the full shaded driver with `-am`
- The shaded JAR relocates all classes under `org.apache.arrow.driver.jdbc.shaded.*`
- JAR timestamps inside shaded JARs show original compile time, not rebuild time — don't trust them
- Maven Central rejects duplicate version uploads — if publish fails and you re-tag, it will fail again
- `concurrency: cancel-in-progress: true` means rapid pushes cancel earlier runs — be careful with tag + main pushes close together
