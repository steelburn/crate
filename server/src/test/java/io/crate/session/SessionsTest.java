/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.session;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import io.crate.analyze.Analyzer;
import io.crate.auth.Protocol;
import io.crate.common.unit.TimeValue;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.transport.CancelRequest;
import io.crate.execution.jobs.transport.TransportCancelAction;
import io.crate.metadata.NodeContext;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Planner;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.protocols.postgres.KeyData;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Securable;
import io.crate.role.metadata.RolesHelper;
import io.crate.sql.tree.Declare.Hold;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class SessionsTest extends CrateDummyClusterServiceUnitTest {

    private final SessionSettingRegistry sessionSettingRegistry = new SessionSettingRegistry(Set.of());

    @Test
    public void test_sessions_broadcasts_cancel_if_no_local_match() throws Exception {
        NodeContext nodeCtx = createNodeContext();
        DependencyCarrier dependencies = mock(DependencyCarrier.class);
        Client client = mock(Client.class, Answers.RETURNS_MOCKS);
        when(dependencies.client()).thenReturn(client);
        Sessions sessions = new Sessions(
            nodeCtx,
            mock(Analyzer.class),
            mock(Planner.class),
            () -> dependencies,
            new JobsLogs(() -> false),
            Settings.EMPTY,
            clusterService,
            sessionSettingRegistry
        );

        KeyData keyData = new KeyData(10, 20);
        sessions.cancel(keyData);
        verify(client).execute(
            Mockito.eq(TransportCancelAction.ACTION),
            Mockito.eq(new CancelRequest(keyData))
        );
    }

    @Test
    public void test_super_user_and_al_privileges_can_view_all_cursors() throws Exception {
        NodeContext nodeCtx = createNodeContext();
        Sessions sessions = newSessions(nodeCtx);
        Session session1 = sessions.newSession(connectionProperties(), "doc", RolesHelper.userOf("Arthur"));
        session1.cursors.add("c1", newCursor());

        Session session2 = sessions.newSession(connectionProperties(), "doc", RolesHelper.userOf("Trillian"));
        session2.cursors.add("c2", newCursor());

        assertThat(sessions.getCursors(Role.CRATE_USER)).hasSize(2);

        var ALprivilege = new Privilege(
            Policy.GRANT,
            Permission.AL,
            Securable.CLUSTER,
            null,
            "crate"
        );
        Role admin = RolesHelper.userOf("admin", Set.of(ALprivilege), null);
        assertThat(sessions.getCursors(admin)).hasSize(2);
    }

    @Test
    public void test_user_can_only_view_their_own_cursors() throws Exception {
        NodeContext nodeCtx = createNodeContext();
        Sessions sessions = newSessions(nodeCtx);

        Role arthur = RolesHelper.userOf("Arthur");
        Session session1 = sessions.newSession(connectionProperties(), "doc", arthur);
        session1.cursors.add("c1", newCursor());

        Role trillian = RolesHelper.userOf("Trillian");
        Session session2 = sessions.newSession(connectionProperties(), "doc", trillian);
        session2.cursors.add("c2", newCursor());

        assertThat(sessions.getCursors(arthur)).hasSize(1);
        assertThat(sessions.getCursors(trillian)).hasSize(1);
    }

    @Test
    public void test_uses_global_statement_timeout_as_default_for() throws Exception {
        NodeContext nodeCtx = createNodeContext();
        Sessions sessions = new Sessions(
            nodeCtx,
            mock(Analyzer.class),
            mock(Planner.class),
            () -> mock(DependencyCarrier.class),
            new JobsLogs(() -> false),
            Settings.builder()
                .put("statement_timeout", "30s")
                .build(),
            clusterService,
            sessionSettingRegistry
        );
        Session session = sessions.newSession(connectionProperties(), "doc", Role.CRATE_USER);
        assertThat(session.sessionSettings().statementTimeout())
            .isEqualTo(TimeValue.timeValueSeconds(30));
    }

    @Test
    public void test_user_session_settings_are_applied() throws Exception {
        Sessions sessions = newSessions(createNodeContext());

        Role john = RolesHelper.userOf("john");
        assertThat(sessions.newSession(connectionProperties(), "", john)
            .sessionSettings().hashJoinsEnabled()).isTrue();
        john = RolesHelper.userOf("john", null).with(null, null, Map.of("enable_hashjoin", false));
        assertThat(sessions.newSession(connectionProperties(), "", john)
            .sessionSettings().hashJoinsEnabled()).isFalse();
    }

    @Test
    public void test_rejects_large_statement() throws Exception {
        Sessions sessions = newSessions(createNodeContext());
        Session session = sessions.newSession(connectionProperties(), "doc", Role.CRATE_USER);
        String longQuery = "SELECT " + "1, ".repeat(200) + "1";
        String expectedMessage =
            "Statement exceeds `statement_max_length` (512 allowed, 608 provided). Try replacing inline values with parameter placeholders (`?`)";
        assertThatThrownBy(() -> session.parse("S1", longQuery, List.of()))
            .hasMessage(expectedMessage);

        assertThatThrownBy(() -> session.quickExec(longQuery, new BaseResultReceiver(), Row.EMPTY))
            .hasMessage(expectedMessage);
    }

    private Sessions newSessions(NodeContext nodeCtx) {
        Sessions sessions = new Sessions(
            nodeCtx,
            mock(Analyzer.class),
            mock(Planner.class),
            () -> mock(DependencyCarrier.class),
            new JobsLogs(() -> false),
            Settings.builder()
                .put("statement_max_length", 512)
                .build(),
            clusterService,
            sessionSettingRegistry
        );
        return sessions;
    }

    private static Cursor newCursor() {
        return new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare ..",
            false,
            Hold.WITH,
            CompletableFuture.completedFuture(InMemoryBatchIterator.empty(null)),
            new CompletableFuture<>(),
            List.of()
        );
    }

    private static ConnectionProperties connectionProperties() {
        return new ConnectionProperties(null, null, Protocol.HTTP, null);
    }
}
