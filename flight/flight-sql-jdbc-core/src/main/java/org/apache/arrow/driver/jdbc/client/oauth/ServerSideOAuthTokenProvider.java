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

import java.awt.Desktop;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth token provider for server-side OAuth code exchange. Calls the GizmoSQL server's {@code
 * /oauth/initiate} endpoint to start the flow, opens a browser for authentication, and polls {@code
 * /oauth/token/{uuid}} for the resulting JWT.
 */
public class ServerSideOAuthTokenProvider implements OAuthTokenProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ServerSideOAuthTokenProvider.class);
  private static final int POLL_INTERVAL_MS = 1000;
  private static final int POLL_TIMEOUT_MS = 300_000; // 5 minutes
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(10);

  /**
   * Shared cache keyed by oauthBaseUrl. Prevents duplicate browser flows when multiple JDBC
   * connections (e.g., from DBeaver) connect to the same server concurrently.
   */
  private static final ConcurrentHashMap<String, SharedState> SHARED_CACHE =
      new ConcurrentHashMap<>();

  private final String oauthBaseUrl;
  private final boolean disableCertificateVerification;
  private final SharedState sharedState;

  /** Shared mutable state for token caching across provider instances with the same key. */
  static class SharedState {
    final Object lock = new Object();
    volatile String cachedToken;
  }

  /**
   * Creates a new server-side OAuth token provider.
   *
   * @param oauthBaseUrl base URL of the GizmoSQL OAuth HTTP server (e.g., {@code
   *     https://host:31339})
   * @param disableCertificateVerification if true, skip TLS certificate verification (for
   *     self-signed certs)
   */
  public ServerSideOAuthTokenProvider(String oauthBaseUrl, boolean disableCertificateVerification) {
    this.oauthBaseUrl = oauthBaseUrl;
    this.disableCertificateVerification = disableCertificateVerification;
    this.sharedState = SHARED_CACHE.computeIfAbsent(oauthBaseUrl, k -> new SharedState());
  }

  @Override
  public String getValidToken() throws SQLException {
    // Check cached token first
    String token = sharedState.cachedToken;
    if (token != null) {
      return token;
    }

    synchronized (sharedState.lock) {
      // Double-check after acquiring lock
      token = sharedState.cachedToken;
      if (token != null) {
        return token;
      }

      token = performServerSideFlow();
      sharedState.cachedToken = token;
      return token;
    }
  }

  private String performServerSideFlow() throws SQLException {
    try {
      HttpClient httpClient = createHttpClient();

      // 1. Call /oauth/initiate
      HttpRequest initiateRequest =
          HttpRequest.newBuilder()
              .uri(URI.create(oauthBaseUrl + "/oauth/initiate"))
              .timeout(HTTP_TIMEOUT)
              .GET()
              .build();

      HttpResponse<String> initiateResponse =
          httpClient.send(initiateRequest, HttpResponse.BodyHandlers.ofString());

      if (initiateResponse.statusCode() != 200) {
        throw new SQLException(
            "OAuth initiate failed with status "
                + initiateResponse.statusCode()
                + ": "
                + initiateResponse.body());
      }

      JSONParser parser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
      JSONObject initiateJson = (JSONObject) parser.parse(initiateResponse.body());

      String sessionUuid = (String) initiateJson.get("session_uuid");
      String authUrl = (String) initiateJson.get("auth_url");

      if (sessionUuid == null || authUrl == null) {
        throw new SQLException("OAuth initiate response missing session_uuid or auth_url");
      }

      // 2. Open browser to auth URL
      LOG.info("Opening browser for server-side SSO login...");
      if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
        Desktop.getDesktop().browse(URI.create(authUrl));
      } else {
        LOG.warn("Cannot open browser automatically. Please open this URL manually:\n{}", authUrl);
        throw new SQLException(
            "Cannot open browser for SSO login. Please open this URL manually: " + authUrl);
      }

      // 3. Poll for token
      LOG.info("Waiting for authentication to complete...");
      long startTime = System.currentTimeMillis();

      while (System.currentTimeMillis() - startTime < POLL_TIMEOUT_MS) {
        Thread.sleep(POLL_INTERVAL_MS);

        HttpRequest pollRequest =
            HttpRequest.newBuilder()
                .uri(URI.create(oauthBaseUrl + "/oauth/token/" + sessionUuid))
                .timeout(HTTP_TIMEOUT)
                .GET()
                .build();

        HttpResponse<String> pollResponse =
            httpClient.send(pollRequest, HttpResponse.BodyHandlers.ofString());

        if (pollResponse.statusCode() != 200) {
          continue;
        }

        JSONObject pollJson = (JSONObject) parser.parse(pollResponse.body());
        String status = (String) pollJson.get("status");

        if ("complete".equals(status)) {
          String resultToken = (String) pollJson.get("token");
          if (resultToken == null || resultToken.isEmpty()) {
            throw new SQLException("OAuth server returned complete status but no token");
          }
          LOG.info("Server-side OAuth authentication successful");
          return resultToken;
        } else if ("error".equals(status)) {
          String error = (String) pollJson.get("error");
          throw new SQLException(
              "Server-side OAuth authentication failed: "
                  + (error != null ? error : "unknown error"));
        }
        // status == "pending" — continue polling
      }

      throw new SQLException(
          "Server-side OAuth authentication timed out after "
              + (POLL_TIMEOUT_MS / 1000)
              + " seconds");

    } catch (SQLException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("OAuth authentication interrupted", e);
    } catch (Exception e) {
      throw new SQLException("Server-side OAuth authentication failed: " + e.getMessage(), e);
    }
  }

  private HttpClient createHttpClient() throws SQLException {
    HttpClient.Builder builder =
        HttpClient.newBuilder()
            .connectTimeout(HTTP_TIMEOUT)
            .followRedirects(HttpClient.Redirect.NORMAL);

    if (disableCertificateVerification) {
      try {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(
            null,
            new TrustManager[] {
              new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {}

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {}

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                  return new X509Certificate[0];
                }
              }
            },
            new SecureRandom());
        builder.sslContext(sslContext);
      } catch (Exception e) {
        throw new SQLException("Failed to configure TLS for OAuth client", e);
      }
    }

    return builder.build();
  }
}
