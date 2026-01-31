<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# GizmoSQL JDBC Driver

[![Maven Central](https://img.shields.io/maven-central/v/com.gizmodata/gizmosql-jdbc-driver)](https://central.sonatype.com/artifact/com.gizmodata/gizmosql-jdbc-driver)

A JDBC driver for [GizmoSQL](https://github.com/gizmodata/gizmosql), based on Arrow Flight SQL.

This project is a fork of [Apache Arrow Java](https://github.com/apache/arrow-java).

## Installation

### Maven

```xml
<dependency>
    <groupId>com.gizmodata</groupId>
    <artifactId>gizmosql-jdbc-driver</artifactId>
    <version>1.3.2</version>
</dependency>
```

### Gradle

```groovy
dependencies {
    implementation 'com.gizmodata:gizmosql-jdbc-driver:1.3.2'
}
```

## Usage

### Basic Connection (with TLS)

```java
import java.sql.*;

public class Example {
    public static void main(String[] args) throws SQLException {
        // Recommended: TLS enabled with certificate verification
        String url = "jdbc:gizmosql://your-server.example.com:31337?useEncryption=true";

        try (Connection conn = DriverManager.getConnection(url, "user", "password");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM my_table")) {

            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }
}
```

### Development/Testing (Skip Certificate Verification)

For development environments with self-signed certificates:

```java
// TLS enabled but skip certificate verification (development only!)
String url = "jdbc:gizmosql://localhost:31337?useEncryption=true&disableCertificateVerification=true";

try (Connection conn = DriverManager.getConnection(url, "user", "password")) {
    // ...
}
```

### Connection URL Format

```
jdbc:gizmosql://[host]:[port]?[param1=value1&param2=value2...]
```

The driver also accepts `jdbc:arrow-flight-sql://` for backward compatibility.

## Connection Properties

| Property | Description | Default |
|----------|-------------|---------|
| **Authentication** | | |
| `user` | Username for user/password authentication | - |
| `password` | Password for user/password authentication | - |
| `token` | Bearer token for token authentication | - |
| **TLS/Encryption** | | |
| `useEncryption` | Whether to use TLS encryption | `true` |
| `disableCertificateVerification` | Skip server certificate verification (dev only) | `false` |
| `useSystemTrustStore` | Use the system certificate store | `true` |
| `trustStore` | Path to Java truststore (JKS) for TLS | - |
| `trustStorePassword` | Password for the truststore | - |
| `tlsRootCerts` | Path to PEM-encoded root certificates (alternative to trustStore) | - |
| **Mutual TLS (mTLS)** | | |
| `clientCertificate` | Path to PEM-encoded client certificate for mTLS | - |
| `clientKey` | Path to PEM-encoded client private key for mTLS | - |
| **Advanced** | | |
| `threadPoolSize` | Size of internal thread pool | `1` |
| `retainCookies` | Retain cookies from initial connection | `true` |
| `retainAuth` | Retain bearer tokens from initial connection | `true` |

**Note:** URI parameter values must be URI-encoded (e.g., `password=my%23password` for `my#password`). Properties object values should not be encoded.

Properties can be passed either in the URL or via the `Properties` object:

```java
Properties props = new Properties();
props.setProperty("user", "myuser");
props.setProperty("password", "mypassword");
props.setProperty("useEncryption", "true");

Connection conn = DriverManager.getConnection("jdbc:gizmosql://localhost:31337", props);
```

## Building from Source

```bash
# Clone the repository
git clone https://github.com/gizmodata/gizmosql-jdbc-driver.git
cd gizmosql-jdbc-driver

# Build the JDBC driver
./mvnw -pl flight/flight-sql-jdbc-driver -am -DskipTests package

# The shaded JAR will be at:
# flight/flight-sql-jdbc-driver/target/gizmosql-jdbc-driver-VERSION.jar
```

## Compatibility

- Java 11 or later
- GizmoSQL server with Flight SQL support

## License

Apache License 2.0 - see [LICENSE.txt](LICENSE.txt)

This project is a fork of Apache Arrow Java. See [NOTICE.txt](NOTICE.txt) for attribution.
