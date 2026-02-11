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

import java.sql.SQLException;
import java.util.Objects;

/**
 * Configuration class for server-side OAuth settings parsed from connection properties.
 *
 * <p>The only supported flow is server-side OAuth ({@code authType=external}), where the GizmoSQL
 * server handles the entire OAuth code exchange as a confidential client.
 */
public class OAuthConfiguration {

  private final String oauthServerUrl;
  private final boolean disableCertificateVerification;

  private OAuthConfiguration(Builder builder) throws SQLException {
    this.oauthServerUrl = builder.oauthServerUrl;
    this.disableCertificateVerification = builder.disableCertificateVerification;

    validate();
  }

  private void validate() throws SQLException {
    Objects.requireNonNull(oauthServerUrl, "oauthServerUrl is required");
    if (oauthServerUrl.isEmpty()) {
      throw new SQLException("oauthServerUrl is required for server-side OAuth");
    }
  }

  /**
   * Creates an OAuthTokenProvider for the server-side OAuth flow.
   *
   * @return the token provider
   */
  public OAuthTokenProvider createTokenProvider() {
    return new ServerSideOAuthTokenProvider(oauthServerUrl, disableCertificateVerification);
  }

  /**
   * Creates an OAuthTokenProvider using the specified URL instead of the configured one. Used when
   * the server advertises its OAuth URL via discovery handshake.
   *
   * @param discoveredUrl the OAuth server URL discovered from the server
   * @return the token provider
   */
  public OAuthTokenProvider createTokenProviderWithUrl(String discoveredUrl) {
    return new ServerSideOAuthTokenProvider(discoveredUrl, disableCertificateVerification);
  }

  /** Builder for OAuthConfiguration. */
  public static class Builder {
    private String oauthServerUrl;
    private boolean disableCertificateVerification;

    /**
     * Sets the OAuth server base URL for server-side flow.
     *
     * @param oauthServerUrl the OAuth server base URL (e.g., {@code https://host:31339})
     * @return this builder
     */
    public Builder oauthServerUrl(String oauthServerUrl) {
      this.oauthServerUrl = oauthServerUrl;
      return this;
    }

    /**
     * Sets whether to disable TLS certificate verification for the OAuth server connection.
     *
     * @param disable true to skip certificate verification
     * @return this builder
     */
    public Builder disableCertificateVerification(boolean disable) {
      this.disableCertificateVerification = disable;
      return this;
    }

    /**
     * Builds the OAuthConfiguration.
     *
     * @return the configuration
     * @throws SQLException if required fields are missing
     */
    public OAuthConfiguration build() throws SQLException {
      return new OAuthConfiguration(this);
    }
  }
}
