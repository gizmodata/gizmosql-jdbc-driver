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
package org.apache.arrow.driver.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

public class ArrowFlightSqlClientHandlerTest {

  private ArrowFlightSqlClientHandler transactionHandler(FlightSqlClient client) {
    return new ArrowFlightSqlClientHandler(
        "transactions",
        client,
        new ArrowFlightSqlClientHandler.Builder(),
        new ArrayList<>(),
        Optional.empty(),
        null);
  }

  @Test
  public void manualTransactionsReachTheServerAndRestartForEachExecution() {
    FlightSqlClient client = mock(FlightSqlClient.class);
    ArrowFlightSqlClientHandler handler = transactionHandler(client);
    handler.setAutoCommit(false);
    handler.setAutoCommit(false);
    handler.executeUpdate("INSERT INTO ledger VALUES (1)");
    handler.rollback();
    handler.executeUpdate("INSERT INTO ledger VALUES (2)");
    handler.commit();
    handler.getInfo("SELECT * FROM ledger");
    handler.setAutoCommit(true);
    InOrder order = inOrder(client);
    order.verify(client).executeUpdate(eq("BEGIN TRANSACTION"), any(CallOption[].class));
    order
        .verify(client)
        .executeUpdate(eq("INSERT INTO ledger VALUES (1)"), any(CallOption[].class));
    order.verify(client).executeUpdate(eq("ROLLBACK"), any(CallOption[].class));
    order.verify(client).executeUpdate(eq("BEGIN TRANSACTION"), any(CallOption[].class));
    order
        .verify(client)
        .executeUpdate(eq("INSERT INTO ledger VALUES (2)"), any(CallOption[].class));
    order.verify(client).executeUpdate(eq("COMMIT"), any(CallOption[].class));
    order.verify(client).executeUpdate(eq("BEGIN TRANSACTION"), any(CallOption[].class));
    order.verify(client).execute(eq("SELECT * FROM ledger"), any(CallOption[].class));
    order.verify(client).executeUpdate(eq("COMMIT"), any(CallOption[].class));
    order.verifyNoMoreInteractions();
  }

  @Test
  public void failedTransactionRestartNeverExecutesAWriteInAutoCommit() {
    FlightSqlClient client = mock(FlightSqlClient.class);
    ArrowFlightSqlClientHandler handler = transactionHandler(client);
    when(client.executeUpdate(eq("BEGIN TRANSACTION"), any(CallOption[].class)))
        .thenReturn(0L)
        .thenThrow(CallStatus.UNAVAILABLE.toRuntimeException())
        .thenReturn(0L);
    handler.setAutoCommit(false);
    handler.commit();
    assertThrows(
        RuntimeException.class, () -> handler.executeUpdate("INSERT INTO ledger VALUES (1)"));
    verify(client, never())
        .executeUpdate(eq("INSERT INTO ledger VALUES (1)"), any(CallOption[].class));
    handler.executeUpdate("INSERT INTO ledger VALUES (1)");
    verify(client).executeUpdate(eq("INSERT INTO ledger VALUES (1)"), any(CallOption[].class));
  }

  @Test
  public void preparedStatementReuseStartsANewTransactionAfterRollback() throws Exception {
    FlightSqlClient client = mock(FlightSqlClient.class);
    FlightSqlClient.PreparedStatement prepared = mock(FlightSqlClient.PreparedStatement.class);
    when(client.prepare(eq("INSERT INTO ledger VALUES (?)"), any(CallOption[].class)))
        .thenReturn(prepared);
    ArrowFlightSqlClientHandler handler = transactionHandler(client);
    handler.setAutoCommit(false);
    ArrowFlightSqlClientHandler.PreparedStatement statement =
        handler.prepare("INSERT INTO ledger VALUES (?)");
    statement.executeUpdate();
    handler.rollback();
    statement.executeUpdate();
    handler.commit();
    InOrder order = inOrder(client, prepared);
    order.verify(client).executeUpdate(eq("BEGIN TRANSACTION"), any(CallOption[].class));
    order.verify(client).prepare(eq("INSERT INTO ledger VALUES (?)"), any(CallOption[].class));
    order.verify(prepared).executeUpdate(any(CallOption[].class));
    order.verify(client).executeUpdate(eq("ROLLBACK"), any(CallOption[].class));
    order.verify(client).executeUpdate(eq("BEGIN TRANSACTION"), any(CallOption[].class));
    order.verify(prepared).executeUpdate(any(CallOption[].class));
    order.verify(client).executeUpdate(eq("COMMIT"), any(CallOption[].class));
    order.verifyNoMoreInteractions();
  }

  @ParameterizedTest
  @MethodSource
  public void testCloseHandlesFlightRuntimeException(
      boolean throwFromCloseSession, CallStatus callStatus, boolean shouldSuppress)
      throws Exception {
    FlightSqlClient sqlClient = mock(FlightSqlClient.class);
    String cacheKey = "cacheKey";
    Optional<String> catalog =
        throwFromCloseSession ? Optional.of("test_catalog") : Optional.empty();
    final Collection<CallOption> credentialOptions = new ArrayList<>();
    ArrowFlightSqlClientHandler.Builder builder = new ArrowFlightSqlClientHandler.Builder();

    if (throwFromCloseSession) {
      doThrow(callStatus.toRuntimeException())
          .when(sqlClient)
          .closeSession(any(CloseSessionRequest.class), any(CallOption[].class));
    } else {
      doThrow(callStatus.toRuntimeException()).when(sqlClient).close();
    }

    ArrowFlightSqlClientHandler sqlClientHandler =
        new ArrowFlightSqlClientHandler(
            cacheKey, sqlClient, builder, credentialOptions, catalog, null);

    if (shouldSuppress) {
      assertDoesNotThrow(sqlClientHandler::close);
    } else {
      assertThrows(SQLException.class, sqlClientHandler::close);
    }
  }

  /**
   * {@link ArrowFlightSqlClientHandler#isSessionValid(int)} is the liveness probe behind {@code
   * Connection.isValid()}. A successful {@code GetSessionOptions} (live session) reports valid; an
   * evicted/unreachable session reports invalid; and — critically for backwards compatibility with
   * older servers — an {@code UNIMPLEMENTED} response reports valid so a healthy connection to a
   * server that doesn't support the probe is never discarded.
   */
  @ParameterizedTest
  @MethodSource
  public void testIsSessionValid(CallStatus probeFailure, boolean expectedValid) {
    FlightSqlClient sqlClient = mock(FlightSqlClient.class);
    if (probeFailure != null) {
      doThrow(probeFailure.toRuntimeException())
          .when(sqlClient)
          .getSessionOptions(any(GetSessionOptionsRequest.class), any(CallOption[].class));
    }
    ArrowFlightSqlClientHandler handler =
        new ArrowFlightSqlClientHandler(
            "cacheKey",
            sqlClient,
            new ArrowFlightSqlClientHandler.Builder(),
            new ArrayList<>(),
            Optional.empty(),
            null);

    assertEquals(expectedValid, handler.isSessionValid(0));
  }

  private static Object[] testIsSessionValid() {
    return new Object[] {
      // Live session: GetSessionOptions returns normally.
      new Object[] {null, true},
      // Session evicted server-side (new GizmoSQL returns UNAUTHENTICATED): recycle.
      new Object[] {new CallStatus(FlightStatusCode.UNAUTHENTICATED, null, null, null), false},
      // Server unreachable: recycle.
      new Object[] {new CallStatus(FlightStatusCode.UNAVAILABLE, null, null, null), false},
      // Server (e.g. older GizmoSQL) doesn't implement the probe: keep the connection.
      new Object[] {new CallStatus(FlightStatusCode.UNIMPLEMENTED, null, null, null), true},
    };
  }

  private static Object[] testCloseHandlesFlightRuntimeException() {
    CallStatus benignInternalError =
        new CallStatus(FlightStatusCode.INTERNAL, null, "Connection closed after GOAWAY", null);
    CallStatus notBenignInternalError =
        new CallStatus(FlightStatusCode.INTERNAL, null, "Not a benign internal error", null);
    CallStatus unavailableError = new CallStatus(FlightStatusCode.UNAVAILABLE, null, null, null);
    CallStatus unimplementedError =
        new CallStatus(FlightStatusCode.UNIMPLEMENTED, null, null, null);
    CallStatus unauthenticatedError =
        new CallStatus(FlightStatusCode.UNAUTHENTICATED, null, null, null);
    CallStatus unknownError = new CallStatus(FlightStatusCode.UNKNOWN, null, null, null);
    return new Object[] {
      new Object[] {true, benignInternalError, true},
      new Object[] {false, benignInternalError, true},
      new Object[] {true, notBenignInternalError, false},
      new Object[] {false, notBenignInternalError, false},
      new Object[] {true, unavailableError, true},
      new Object[] {false, unavailableError, true},
      new Object[] {true, unimplementedError, true},
      new Object[] {false, unimplementedError, true},
      new Object[] {true, unauthenticatedError, true},
      new Object[] {false, unauthenticatedError, true},
      new Object[] {true, unknownError, false},
      new Object[] {false, unknownError, false},
    };
  }
}
