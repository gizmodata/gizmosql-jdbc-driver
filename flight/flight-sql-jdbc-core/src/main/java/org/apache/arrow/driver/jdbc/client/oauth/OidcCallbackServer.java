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

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Temporary localhost HTTP server that handles the OAuth callback redirect. Binds to a random
 * available port on 127.0.0.1, waits for the authorization code callback, and shuts down
 * automatically.
 */
public class OidcCallbackServer implements AutoCloseable {

  private static final String CALLBACK_PATH = "/callback";
  private static final int DEFAULT_TIMEOUT_SECONDS = 120;

  private static final String LOGO_BASE64 = loadLogoBase64();

  private static final String SUCCESS_HTML =
      "<!DOCTYPE html><html><head><title>GizmoSQL - Login Successful</title>"
          + "<style>body{font-family:system-ui,sans-serif;display:flex;justify-content:center;"
          + "align-items:center;height:100vh;margin:0;background:#f0f9ff}"
          + ".card{background:white;padding:2rem;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,0.1);"
          + "text-align:center;max-width:400px}"
          + ".logo{width:96px;height:96px;margin-bottom:1rem}"
          + "h1{color:#059669;margin-bottom:0.5rem}"
          + "p{color:#6b7280}</style></head>"
          + "<body><div class='card'>"
          + "<img class='logo' src='data:image/png;base64,"
          + LOGO_BASE64
          + "' alt='GizmoSQL'/>"
          + "<h1>Login Successful</h1>"
          + "<p>You can close this window and return to your application.</p>"
          + "</div></body></html>";

  private final HttpServer server;
  private final CompletableFuture<String> authCodeFuture;
  private final String expectedState;

  private OidcCallbackServer(
      HttpServer server, CompletableFuture<String> authCodeFuture, String expectedState) {
    this.server = server;
    this.authCodeFuture = authCodeFuture;
    this.expectedState = expectedState;
  }

  /**
   * Start a callback server on a random available port.
   *
   * @param expectedState the state parameter to verify in the callback
   * @return a new OidcCallbackServer instance
   * @throws IOException if the server cannot be started
   */
  public static OidcCallbackServer start(String expectedState) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    CompletableFuture<String> authCodeFuture = new CompletableFuture<>();

    OidcCallbackServer callbackServer =
        new OidcCallbackServer(server, authCodeFuture, expectedState);

    server.createContext(
        CALLBACK_PATH,
        exchange -> {
          try {
            URI requestUri = exchange.getRequestURI();
            Map<String, String> params = parseQueryParams(requestUri.getQuery());

            String error = params.get("error");
            if (error != null) {
              String errorDesc = params.getOrDefault("error_description", "Unknown error");
              String html =
                  "<!DOCTYPE html><html><body><h1>Login Failed</h1>"
                      + "<p>"
                      + error
                      + ": "
                      + errorDesc
                      + "</p></body></html>";
              byte[] response = html.getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
              exchange.sendResponseHeaders(400, response.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
              }
              authCodeFuture.completeExceptionally(
                  new SQLException("OAuth authorization failed: " + error + " - " + errorDesc));
              return;
            }

            String state = params.get("state");
            if (state == null || !state.equals(callbackServer.expectedState)) {
              String html =
                  "<!DOCTYPE html><html><body><h1>Error</h1>"
                      + "<p>Invalid state parameter.</p></body></html>";
              byte[] response = html.getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
              exchange.sendResponseHeaders(400, response.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
              }
              authCodeFuture.completeExceptionally(
                  new SQLException("OAuth callback state mismatch (possible CSRF attack)"));
              return;
            }

            String code = params.get("code");
            if (code == null || code.isEmpty()) {
              String html =
                  "<!DOCTYPE html><html><body><h1>Error</h1>"
                      + "<p>No authorization code received.</p></body></html>";
              byte[] response = html.getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
              exchange.sendResponseHeaders(400, response.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
              }
              authCodeFuture.completeExceptionally(
                  new SQLException("No authorization code in OAuth callback"));
              return;
            }

            // Success - serve the success page and complete the future
            byte[] response = SUCCESS_HTML.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(response);
            }
            authCodeFuture.complete(code);
          } catch (Exception e) {
            authCodeFuture.completeExceptionally(e);
          }
        });

    server.setExecutor(null);
    server.start();
    return callbackServer;
  }

  /**
   * Get the port this server is listening on.
   *
   * @return the port number
   */
  public int getPort() {
    return server.getAddress().getPort();
  }

  /**
   * Get the redirect URI for this callback server.
   *
   * @return the callback URI (e.g., http://127.0.0.1:12345/callback)
   */
  public URI getRedirectUri() {
    return URI.create("http://127.0.0.1:" + getPort() + CALLBACK_PATH);
  }

  /**
   * Wait for the authorization code from the callback.
   *
   * @return the authorization code
   * @throws SQLException if the code is not received within the timeout or an error occurs
   */
  public String waitForAuthorizationCode() throws SQLException {
    return waitForAuthorizationCode(DEFAULT_TIMEOUT_SECONDS);
  }

  /**
   * Wait for the authorization code from the callback with a custom timeout.
   *
   * @param timeoutSeconds the maximum time to wait
   * @return the authorization code
   * @throws SQLException if the code is not received within the timeout or an error occurs
   */
  public String waitForAuthorizationCode(int timeoutSeconds) throws SQLException {
    try {
      return authCodeFuture.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new SQLException(
          "Timed out waiting for browser login (waited "
              + timeoutSeconds
              + " seconds). "
              + "Please try connecting again.",
          e);
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw new SQLException("Failed to receive authorization code", e);
    }
  }

  @Override
  public void close() {
    server.stop(1);
  }

  private static String loadLogoBase64() {
    try (InputStream is = OidcCallbackServer.class.getResourceAsStream("/gizmosql_logo.png")) {
      if (is != null) {
        return Base64.getEncoder().encodeToString(is.readAllBytes());
      }
    } catch (IOException e) {
      // Fall through to empty string
    }
    return "";
  }

  private static Map<String, String> parseQueryParams(String query) {
    Map<String, String> params = new HashMap<>();
    if (query == null || query.isEmpty()) {
      return params;
    }
    for (String pair : query.split("&")) {
      int idx = pair.indexOf('=');
      if (idx > 0) {
        String key = pair.substring(0, idx);
        String value = idx < pair.length() - 1 ? pair.substring(idx + 1) : "";
        params.put(key, value);
      }
    }
    return params;
  }
}
