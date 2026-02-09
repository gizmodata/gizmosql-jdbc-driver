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

import com.nimbusds.jwt.JWT;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.RefreshTokenGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.pkce.CodeVerifier;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth token provider that implements the Authorization Code flow with PKCE. Opens a browser for
 * the user to authenticate with their Identity Provider, then exchanges the authorization code for
 * tokens.
 */
public class AuthorizationCodeTokenProvider implements OAuthTokenProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationCodeTokenProvider.class);
  private static final int EXPIRATION_BUFFER_SECONDS = 30;
  private static final int DEFAULT_EXPIRATION_SECONDS = 3600;

  /**
   * Shared token cache keyed by (tokenEndpoint, clientId). This ensures that multiple JDBC
   * connections to the same IdP (e.g., from DBeaver opening parallel connections) share the same
   * tokens instead of each triggering a separate browser login flow.
   */
  private static final ConcurrentHashMap<String, SharedTokenState> SHARED_CACHE =
      new ConcurrentHashMap<>();

  private final URI authorizationEndpoint;
  private final URI tokenEndpoint;
  private final String clientId;
  private final @Nullable String clientSecret;
  private final @Nullable String scope;
  private final SharedTokenState sharedState;

  /** Shared mutable state for token caching across provider instances with the same key. */
  static class SharedTokenState {
    final Object tokenLock = new Object();
    volatile @Nullable TokenInfo cachedToken;
    volatile @Nullable RefreshToken refreshToken;
  }

  AuthorizationCodeTokenProvider(
      URI authorizationEndpoint,
      URI tokenEndpoint,
      String clientId,
      @Nullable String clientSecret,
      @Nullable String scope) {
    this.authorizationEndpoint =
        Objects.requireNonNull(authorizationEndpoint, "authorizationEndpoint cannot be null");
    this.tokenEndpoint = Objects.requireNonNull(tokenEndpoint, "tokenEndpoint cannot be null");
    this.clientId = Objects.requireNonNull(clientId, "clientId cannot be null");
    this.clientSecret = clientSecret;
    this.scope = scope;

    String cacheKey = tokenEndpoint.toString() + "|" + clientId;
    this.sharedState = SHARED_CACHE.computeIfAbsent(cacheKey, k -> new SharedTokenState());
  }

  @Override
  public String getValidToken() throws SQLException {
    TokenInfo token = sharedState.cachedToken;
    if (token != null && !token.isExpired(EXPIRATION_BUFFER_SECONDS)) {
      return token.getAccessToken();
    }

    synchronized (sharedState.tokenLock) {
      token = sharedState.cachedToken;
      if (token != null && !token.isExpired(EXPIRATION_BUFFER_SECONDS)) {
        return token.getAccessToken();
      }

      // Try refresh first
      if (sharedState.refreshToken != null) {
        try {
          TokenInfo refreshed = refreshAccessToken();
          if (refreshed != null) {
            sharedState.cachedToken = refreshed;
            return refreshed.getAccessToken();
          }
        } catch (SQLException e) {
          LOG.debug("Refresh token failed, falling back to browser flow", e);
          sharedState.refreshToken = null;
        }
      }

      // Full browser flow
      TokenInfo newToken = performBrowserFlow();
      sharedState.cachedToken = newToken;
      return newToken.getAccessToken();
    }
  }

  private TokenInfo performBrowserFlow() throws SQLException {
    PkceGenerator pkce = new PkceGenerator();
    State state = new State();

    try (OidcCallbackServer callbackServer = OidcCallbackServer.start(state.getValue())) {
      URI redirectUri = callbackServer.getRedirectUri();

      // Build the authorization URL
      StringBuilder authUrl = new StringBuilder(authorizationEndpoint.toString());
      authUrl.append(authorizationEndpoint.toString().contains("?") ? "&" : "?");
      authUrl.append("response_type=code");
      authUrl.append("&client_id=").append(encodeUriComponent(clientId));
      authUrl.append("&redirect_uri=").append(encodeUriComponent(redirectUri.toString()));
      authUrl.append("&state=").append(encodeUriComponent(state.getValue()));
      authUrl.append("&code_challenge=").append(encodeUriComponent(pkce.getCodeChallenge()));
      authUrl.append("&code_challenge_method=").append(pkce.getCodeChallengeMethod());
      if (scope != null && !scope.isEmpty()) {
        authUrl.append("&scope=").append(encodeUriComponent(scope));
      }

      URI authUri = URI.create(authUrl.toString());
      LOG.info("Opening browser for SSO login...");

      // Open browser
      if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
        Desktop.getDesktop().browse(authUri);
      } else {
        // Fallback: print URL for manual navigation
        LOG.warn("Cannot open browser automatically. Please open this URL manually:\n{}", authUri);
        throw new SQLException(
            "Cannot open browser for SSO login. Please open this URL manually: " + authUri);
      }

      // Wait for the authorization code
      String authCode = callbackServer.waitForAuthorizationCode();
      LOG.debug("Received authorization code, exchanging for tokens...");

      // Exchange authorization code for tokens
      return exchangeCodeForTokens(authCode, redirectUri, pkce.getCodeVerifier());
    } catch (IOException e) {
      throw new SQLException("Failed to start OAuth callback server", e);
    }
  }

  private TokenInfo exchangeCodeForTokens(String authCode, URI redirectUri, String codeVerifier)
      throws SQLException {
    try {
      AuthorizationCodeGrant grant =
          new AuthorizationCodeGrant(
              new AuthorizationCode(authCode), redirectUri, new CodeVerifier(codeVerifier));

      TokenRequest request;
      if (clientSecret != null && !clientSecret.isEmpty()) {
        // Some IdPs (e.g., Google) require a client secret even for desktop/public apps
        request =
            new TokenRequest(
                tokenEndpoint,
                new ClientSecretPost(new ClientID(clientId), new Secret(clientSecret)),
                grant);
      } else {
        request = new TokenRequest(tokenEndpoint, new ClientID(clientId), grant);
      }

      // Parse as OIDC token response to get the ID token (if present)
      TokenResponse response = OIDCTokenResponseParser.parse(request.toHTTPRequest().send());

      if (!response.indicatesSuccess()) {
        TokenErrorResponse errorResponse = response.toErrorResponse();
        throw new SQLException(
            String.format(
                "Token exchange failed: %s - %s",
                errorResponse.getErrorObject().getCode(),
                errorResponse.getErrorObject().getDescription()));
      }

      var successResponse = response.toSuccessResponse().getTokens();
      var accessToken = successResponse.getAccessToken();
      var newRefreshToken = successResponse.getRefreshToken();

      if (newRefreshToken != null) {
        this.sharedState.refreshToken = newRefreshToken;
      }

      // Prefer the ID token (JWT) over the access token. Some IdPs (e.g., Google)
      // return opaque access tokens that cannot be verified via JWKS. The ID token
      // is always a JWT with standard claims (iss, aud, sub) that the server can validate.
      String tokenValue;
      long expiresIn;
      if (response instanceof OIDCTokenResponse) {
        JWT idToken = ((OIDCTokenResponse) response).getOIDCTokens().getIDToken();
        if (idToken != null) {
          tokenValue = idToken.serialize();
          // ID tokens typically have shorter lifetimes; use access token lifetime as fallback
          expiresIn =
              accessToken.getLifetime() > 0
                  ? accessToken.getLifetime()
                  : DEFAULT_EXPIRATION_SECONDS;
          LOG.info("Successfully obtained ID token via authorization code flow");
        } else {
          tokenValue = accessToken.getValue();
          expiresIn =
              accessToken.getLifetime() > 0
                  ? accessToken.getLifetime()
                  : DEFAULT_EXPIRATION_SECONDS;
          LOG.info("Successfully obtained access token via authorization code flow (no ID token)");
        }
      } else {
        tokenValue = accessToken.getValue();
        expiresIn =
            accessToken.getLifetime() > 0 ? accessToken.getLifetime() : DEFAULT_EXPIRATION_SECONDS;
        LOG.info("Successfully obtained access token via authorization code flow");
      }

      Instant expiresAt = Instant.now().plusSeconds(expiresIn);
      return new TokenInfo(tokenValue, expiresAt);
    } catch (ParseException e) {
      throw new SQLException("Failed to parse token response", e);
    } catch (IOException e) {
      throw new SQLException("Failed to exchange authorization code for tokens", e);
    }
  }

  private @Nullable TokenInfo refreshAccessToken() throws SQLException {
    if (sharedState.refreshToken == null) {
      return null;
    }
    try {
      RefreshTokenGrant grant = new RefreshTokenGrant(sharedState.refreshToken);
      Scope requestScope = (scope != null && !scope.isEmpty()) ? Scope.parse(scope) : null;

      TokenRequest.Builder builder;
      if (clientSecret != null && !clientSecret.isEmpty()) {
        builder =
            new TokenRequest.Builder(
                tokenEndpoint,
                new ClientSecretPost(new ClientID(clientId), new Secret(clientSecret)),
                grant);
      } else {
        builder = new TokenRequest.Builder(tokenEndpoint, new ClientID(clientId), grant);
      }
      if (requestScope != null) {
        builder.scope(requestScope);
      }
      TokenRequest request = builder.build();

      // Parse as OIDC token response to get the ID token (if present)
      TokenResponse response = OIDCTokenResponseParser.parse(request.toHTTPRequest().send());

      if (!response.indicatesSuccess()) {
        // Refresh failed - caller should fall back to browser flow
        LOG.debug("Refresh token request failed, will re-authenticate");
        return null;
      }

      var successResponse = response.toSuccessResponse().getTokens();
      var accessToken = successResponse.getAccessToken();
      var newRefreshToken = successResponse.getRefreshToken();

      if (newRefreshToken != null) {
        this.sharedState.refreshToken = newRefreshToken;
      }

      // Prefer ID token (JWT) over opaque access token, same as initial exchange
      String tokenValue;
      long expiresIn;
      if (response instanceof OIDCTokenResponse) {
        JWT idToken = ((OIDCTokenResponse) response).getOIDCTokens().getIDToken();
        if (idToken != null) {
          tokenValue = idToken.serialize();
          expiresIn =
              accessToken.getLifetime() > 0
                  ? accessToken.getLifetime()
                  : DEFAULT_EXPIRATION_SECONDS;
          LOG.debug("Successfully refreshed ID token");
        } else {
          tokenValue = accessToken.getValue();
          expiresIn =
              accessToken.getLifetime() > 0
                  ? accessToken.getLifetime()
                  : DEFAULT_EXPIRATION_SECONDS;
          LOG.debug("Successfully refreshed access token (no ID token)");
        }
      } else {
        tokenValue = accessToken.getValue();
        expiresIn =
            accessToken.getLifetime() > 0 ? accessToken.getLifetime() : DEFAULT_EXPIRATION_SECONDS;
        LOG.debug("Successfully refreshed access token");
      }

      Instant expiresAt = Instant.now().plusSeconds(expiresIn);
      return new TokenInfo(tokenValue, expiresAt);
    } catch (ParseException e) {
      throw new SQLException("Failed to parse refresh token response", e);
    } catch (IOException e) {
      throw new SQLException("Failed to send refresh token request", e);
    }
  }

  private static String encodeUriComponent(String value) {
    try {
      return java.net.URLEncoder.encode(value, "UTF-8");
    } catch (java.io.UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 not supported", e);
    }
  }
}
