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

A JDBC driver for [GizmoSQL](https://github.com/gizmodata/gizmosql), based on Arrow Flight SQL.

This project is a fork of [Apache Arrow Java](https://github.com/apache/arrow-java).

## Installation

### Maven

```xml
<dependency>
    <groupId>com.gizmodata</groupId>
    <artifactId>gizmosql-jdbc-driver</artifactId>
    <version>VERSION</version>
</dependency>

<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/gizmodata/gizmosql-jdbc-driver</url>
    </repository>
</repositories>
```

### Gradle

```groovy
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/gizmodata/gizmosql-jdbc-driver")
        credentials {
            username = project.findProperty("gpr.user") ?: System.getenv("GITHUB_ACTOR")
            password = project.findProperty("gpr.key") ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation 'com.gizmodata:gizmosql-jdbc-driver:VERSION'
}
```

## Usage

```java
import java.sql.*;

public class Example {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:arrow-flight-sql://localhost:31337";

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

## Connection Properties

| Property | Description |
|----------|-------------|
| `user` | Username for authentication |
| `password` | Password for authentication |
| `useEncryption` | Enable TLS encryption (default: false) |
| `disableCertificateVerification` | Skip certificate verification (default: false) |
| `token` | Bearer token for authentication |
| `trustStore` | Path to Java truststore for TLS |
| `trustStorePassword` | Truststore password |

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
