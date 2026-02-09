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

import com.nimbusds.oauth2.sdk.GrantType;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration class for OAuth settings parsed from connection properties. */
public class OAuthConfiguration {

  /** Sentinel grant type for the authorization code flow (PKCE). */
  static final GrantType AUTHORIZATION_CODE_GRANT = new GrantType("authorization_code");

  private final GrantType grantType;
  private final @Nullable URI tokenUri;
  private final @Nullable String clientId;
  private final @Nullable String clientSecret;
  private final @Nullable String scope;
  private final @Nullable String subjectToken;
  private final @Nullable String subjectTokenType;
  private final @Nullable String actorToken;
  private final @Nullable String actorTokenType;
  private final @Nullable String audience;
  private final @Nullable String resource;
  private final @Nullable String requestedTokenType;
  private final @Nullable URI authorizationUri;
  private final @Nullable String oidcIssuer;

  private OAuthConfiguration(Builder builder) throws SQLException {
    this.grantType = builder.grantType;
    this.tokenUri = builder.tokenUri;
    this.clientId = builder.clientId;
    this.clientSecret = builder.clientSecret;
    this.scope = builder.scope;
    this.subjectToken = builder.subjectToken;
    this.subjectTokenType = builder.subjectTokenType;
    this.actorToken = builder.actorToken;
    this.actorTokenType = builder.actorTokenType;
    this.audience = builder.audience;
    this.resource = builder.resource;
    this.requestedTokenType = builder.requestedTokenType;
    this.authorizationUri = builder.authorizationUri;
    this.oidcIssuer = builder.oidcIssuer;

    validate();
  }

  private void validate() throws SQLException {
    Objects.requireNonNull(grantType, "OAuth grant type is required");

    if (AUTHORIZATION_CODE_GRANT.equals(grantType)) {
      // Authorization code flow: requires clientId; tokenUri and authorizationUri
      // are either explicit or discovered from oidcIssuer
      if (clientId == null || clientId.isEmpty()) {
        throw new SQLException("clientId is required for authorization_code flow");
      }
      if (tokenUri == null && (oidcIssuer == null || oidcIssuer.isEmpty())) {
        throw new SQLException(
            "Either tokenUri+authorizationUrl or oidc.issuer is required for authorization_code flow");
      }
    } else {
      Objects.requireNonNull(tokenUri, "Token URI is required");

      if (GrantType.CLIENT_CREDENTIALS.equals(grantType)) {
        if (clientId == null || clientId.isEmpty()) {
          throw new SQLException("clientId is required for client_credentials flow");
        }
        if (clientSecret == null || clientSecret.isEmpty()) {
          throw new SQLException("clientSecret is required for client_credentials flow");
        }
      } else if (GrantType.TOKEN_EXCHANGE.equals(grantType)) {
        if (subjectToken == null || subjectToken.isEmpty()) {
          throw new SQLException("subjectToken is required for token_exchange flow");
        }
        if (subjectTokenType == null || subjectTokenType.isEmpty()) {
          throw new SQLException("subjectTokenType is required for token_exchange flow");
        }
      } else {
        throw new SQLException("Unsupported OAuth grant type: " + grantType);
      }
    }
  }

  /**
   * Creates an OAuthTokenProvider based on the configured grant type.
   *
   * @return the token provider
   * @throws SQLException if the grant type is not supported or configuration is invalid
   */
  public OAuthTokenProvider createTokenProvider() throws SQLException {
    if (AUTHORIZATION_CODE_GRANT.equals(grantType)) {
      // Resolve endpoints: either explicit or via OIDC discovery
      URI resolvedAuthUri = authorizationUri;
      URI resolvedTokenUri = tokenUri;

      if ((resolvedAuthUri == null || resolvedTokenUri == null)
          && oidcIssuer != null
          && !oidcIssuer.isEmpty()) {
        OidcDiscovery discovery = OidcDiscovery.discover(oidcIssuer);
        if (resolvedAuthUri == null) {
          resolvedAuthUri = discovery.getAuthorizationEndpoint();
        }
        if (resolvedTokenUri == null) {
          resolvedTokenUri = discovery.getTokenEndpoint();
        }
      }

      if (resolvedAuthUri == null) {
        throw new SQLException(
            "Authorization endpoint could not be determined. "
                + "Set oauth.authorizationUrl or oidc.issuer.");
      }
      if (resolvedTokenUri == null) {
        throw new SQLException(
            "Token endpoint could not be determined. Set oauth.tokenUri or oidc.issuer.");
      }

      return new AuthorizationCodeTokenProvider(
          resolvedAuthUri, resolvedTokenUri, clientId, clientSecret, scope);
    } else if (GrantType.CLIENT_CREDENTIALS.equals(grantType)) {
      return OAuthTokenProviders.clientCredentials()
          .tokenUri(tokenUri)
          .clientId(clientId)
          .clientSecret(clientSecret)
          .scope(scope)
          .build();
    } else if (GrantType.TOKEN_EXCHANGE.equals(grantType)) {
      OAuthTokenProviders.TokenExchangeBuilder builder =
          OAuthTokenProviders.tokenExchange()
              .tokenUri(tokenUri)
              .subjectToken(subjectToken)
              .subjectTokenType(subjectTokenType)
              .actorToken(actorToken)
              .actorTokenType(actorTokenType)
              .audience(audience)
              .requestedTokenType(requestedTokenType)
              .scope(scope)
              .resource(resource);

      if (clientId != null && clientSecret != null) {
        builder.clientCredentials(clientId, clientSecret);
      }

      return builder.build();
    } else {
      throw new SQLException("Unsupported OAuth grant type: " + grantType);
    }
  }

  /** Builder for OAuthConfiguration. */
  public static class Builder {
    private GrantType grantType;
    private @Nullable URI tokenUri;
    private @Nullable String clientId;
    private @Nullable String clientSecret;
    private @Nullable String scope;
    private @Nullable String subjectToken;
    private @Nullable String subjectTokenType;
    private @Nullable String actorToken;
    private @Nullable String actorTokenType;
    private @Nullable String audience;
    private @Nullable String resource;
    private @Nullable String requestedTokenType;
    private @Nullable URI authorizationUri;
    private @Nullable String oidcIssuer;

    /**
     * Sets the OAuth grant type from a string value.
     *
     * <p>Accepts either user-friendly names ("client_credentials", "token_exchange") or the full
     * URN format as defined in RFC 6749 and RFC 8693.
     *
     * @param flowStr the flow type string (e.g., "client_credentials", "token_exchange")
     * @return this builder
     * @throws SQLException if the flow string is invalid
     */
    public Builder flow(String flowStr) throws SQLException {
      if (flowStr == null || flowStr.isEmpty()) {
        throw new SQLException("OAuth flow cannot be null or empty");
      }
      try {
        String normalized = flowStr.toLowerCase(Locale.ROOT);
        // Map user-friendly names
        if ("token_exchange".equals(normalized)) {
          normalized = GrantType.TOKEN_EXCHANGE.getValue();
        }
        if ("authorization_code".equals(normalized)) {
          this.grantType = AUTHORIZATION_CODE_GRANT;
        } else {
          GrantType parsed = GrantType.parse(normalized);
          if (!parsed.equals(GrantType.CLIENT_CREDENTIALS)
              && !parsed.equals(GrantType.TOKEN_EXCHANGE)) {
            throw new SQLException("Unsupported OAuth flow: " + flowStr);
          }
          this.grantType = parsed;
        }
      } catch (com.nimbusds.oauth2.sdk.ParseException e) {
        throw new SQLException("Invalid OAuth flow: " + flowStr, e);
      }
      return this;
    }

    /**
     * Sets the token URI.
     *
     * @param tokenUri the OAuth token endpoint URI
     * @return this builder
     * @throws SQLException if the URI is invalid
     */
    public Builder tokenUri(@Nullable String tokenUri) throws SQLException {
      if (tokenUri == null || tokenUri.isEmpty()) {
        this.tokenUri = null;
        return this;
      }
      try {
        this.tokenUri = new URI(tokenUri);
      } catch (URISyntaxException e) {
        throw new SQLException("Invalid token URI: " + tokenUri, e);
      }
      return this;
    }

    public Builder authorizationUrl(@Nullable String authorizationUrl) throws SQLException {
      if (authorizationUrl == null || authorizationUrl.isEmpty()) {
        this.authorizationUri = null;
        return this;
      }
      try {
        this.authorizationUri = new URI(authorizationUrl);
      } catch (URISyntaxException e) {
        throw new SQLException("Invalid authorization URL: " + authorizationUrl, e);
      }
      return this;
    }

    public Builder oidcIssuer(@Nullable String oidcIssuer) {
      this.oidcIssuer = oidcIssuer;
      return this;
    }

    public Builder clientId(@Nullable String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder clientSecret(@Nullable String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public Builder scope(@Nullable String scope) {
      this.scope = scope;
      return this;
    }

    public Builder subjectToken(@Nullable String subjectToken) {
      this.subjectToken = subjectToken;
      return this;
    }

    public Builder subjectTokenType(@Nullable String subjectTokenType) {
      this.subjectTokenType = subjectTokenType;
      return this;
    }

    public Builder actorToken(@Nullable String actorToken) {
      this.actorToken = actorToken;
      return this;
    }

    public Builder actorTokenType(@Nullable String actorTokenType) {
      this.actorTokenType = actorTokenType;
      return this;
    }

    public Builder audience(@Nullable String audience) {
      this.audience = audience;
      return this;
    }

    public Builder resource(@Nullable String resource) {
      this.resource = resource;
      return this;
    }

    public Builder requestedTokenType(@Nullable String requestedTokenType) {
      this.requestedTokenType = requestedTokenType;
      return this;
    }

    public OAuthConfiguration build() throws SQLException {
      return new OAuthConfiguration(this);
    }
  }
}
