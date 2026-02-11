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
package org.apache.arrow.driver.jdbc.client.oauth;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/** Tests for {@link OAuthConfiguration}. */
public class OAuthConfigurationTest {

  @Test
  public void testCreateServerSideTokenProvider() throws SQLException {
    OAuthConfiguration config =
        new OAuthConfiguration.Builder()
            .oauthServerUrl("https://localhost:31339")
            .disableCertificateVerification(true)
            .build();

    OAuthTokenProvider provider = config.createTokenProvider();
    assertNotNull(provider);
    assertInstanceOf(ServerSideOAuthTokenProvider.class, provider);
  }

  @Test
  public void testMissingOauthServerUrl() {
    assertThrows(
        NullPointerException.class,
        () -> new OAuthConfiguration.Builder().disableCertificateVerification(true).build());
  }

  @Test
  public void testEmptyOauthServerUrl() {
    assertThrows(
        SQLException.class,
        () ->
            new OAuthConfiguration.Builder()
                .oauthServerUrl("")
                .disableCertificateVerification(true)
                .build());
  }
}
