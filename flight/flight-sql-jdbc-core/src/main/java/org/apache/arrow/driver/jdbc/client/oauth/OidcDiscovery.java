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

import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;

/**
 * Fetches and parses the OIDC discovery document from an issuer's .well-known/openid-configuration
 * endpoint.
 */
public class OidcDiscovery {

  private final URI authorizationEndpoint;
  private final URI tokenEndpoint;

  private OidcDiscovery(URI authorizationEndpoint, URI tokenEndpoint) {
    this.authorizationEndpoint = authorizationEndpoint;
    this.tokenEndpoint = tokenEndpoint;
  }

  /**
   * Discover OIDC endpoints from the issuer URL.
   *
   * @param issuerUrl the OIDC issuer URL (e.g., https://accounts.google.com)
   * @return an OidcDiscovery instance with discovered endpoints
   * @throws SQLException if discovery fails
   */
  public static OidcDiscovery discover(String issuerUrl) throws SQLException {
    try {
      // Normalize: strip trailing slash
      String base =
          issuerUrl.endsWith("/") ? issuerUrl.substring(0, issuerUrl.length() - 1) : issuerUrl;

      Issuer issuer = new Issuer(base);
      OIDCProviderMetadata metadata = OIDCProviderMetadata.resolve(issuer);

      URI authEndpoint = metadata.getAuthorizationEndpointURI();
      URI tokenEndpoint = metadata.getTokenEndpointURI();

      if (authEndpoint == null) {
        throw new SQLException(
            "OIDC discovery document from " + issuerUrl + " missing authorization_endpoint");
      }
      if (tokenEndpoint == null) {
        throw new SQLException(
            "OIDC discovery document from " + issuerUrl + " missing token_endpoint");
      }

      return new OidcDiscovery(authEndpoint, tokenEndpoint);
    } catch (com.nimbusds.oauth2.sdk.GeneralException e) {
      throw new SQLException("Failed to fetch OIDC discovery document from " + issuerUrl, e);
    } catch (IOException e) {
      throw new SQLException("Failed to connect to OIDC issuer at " + issuerUrl, e);
    }
  }

  public URI getAuthorizationEndpoint() {
    return authorizationEndpoint;
  }

  public URI getTokenEndpoint() {
    return tokenEndpoint;
  }
}
