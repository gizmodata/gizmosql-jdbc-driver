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

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;

/**
 * Flight client middleware that captures the {@code x-gizmosql-oauth-url} response header sent by
 * the GizmoSQL server during an OAuth discovery handshake.
 *
 * <p>When the client sends a Handshake with {@code username="__discover__"}, the server responds
 * with the OAuth HTTP server URL in this custom header. The middleware captures it so the JDBC
 * driver can use the correct URL (including scheme) for the OAuth flow.
 */
public class OAuthDiscoveryMiddleware implements FlightClientMiddleware {

  private static final String OAUTH_URL_HEADER = "x-gizmosql-oauth-url";

  private final Factory factory;

  OAuthDiscoveryMiddleware(Factory factory) {
    this.factory = factory;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

  @Override
  public void onHeadersReceived(CallHeaders incomingHeaders) {
    String url = incomingHeaders.get(OAUTH_URL_HEADER);
    if (url != null && !url.isEmpty()) {
      factory.setDiscoveredOAuthUrl(url);
    }
  }

  @Override
  public void onCallCompleted(CallStatus status) {}

  /** Factory that creates {@link OAuthDiscoveryMiddleware} instances and stores discovered URLs. */
  public static class Factory implements FlightClientMiddleware.Factory {
    private volatile String discoveredOAuthUrl;

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      return new OAuthDiscoveryMiddleware(this);
    }

    /**
     * Returns the OAuth URL discovered from the server, or null if discovery has not occurred.
     *
     * @return the discovered OAuth URL, or null
     */
    public String getDiscoveredOAuthUrl() {
      return discoveredOAuthUrl;
    }

    void setDiscoveredOAuthUrl(String url) {
      this.discoveredOAuthUrl = url;
    }
  }
}
