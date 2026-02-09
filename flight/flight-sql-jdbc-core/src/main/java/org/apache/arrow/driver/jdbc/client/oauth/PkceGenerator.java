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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Generates PKCE (Proof Key for Code Exchange) code verifier and challenge pairs as defined in RFC
 * 7636.
 */
public class PkceGenerator {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int DEFAULT_VERIFIER_LENGTH = 64;
  private static final String UNRESERVED_CHARS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";

  private final String codeVerifier;
  private final String codeChallenge;

  public PkceGenerator() {
    this(DEFAULT_VERIFIER_LENGTH);
  }

  public PkceGenerator(int verifierLength) {
    if (verifierLength < 43 || verifierLength > 128) {
      throw new IllegalArgumentException("Verifier length must be between 43 and 128 characters");
    }
    this.codeVerifier = generateVerifier(verifierLength);
    this.codeChallenge = computeChallenge(this.codeVerifier);
  }

  public String getCodeVerifier() {
    return codeVerifier;
  }

  public String getCodeChallenge() {
    return codeChallenge;
  }

  public String getCodeChallengeMethod() {
    return "S256";
  }

  private static String generateVerifier(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(UNRESERVED_CHARS.charAt(RANDOM.nextInt(UNRESERVED_CHARS.length())));
    }
    return sb.toString();
  }

  static String computeChallenge(String verifier) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(verifier.getBytes(StandardCharsets.US_ASCII));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }
}
