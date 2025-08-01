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

package io.crate.metadata.information;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.metadata.RelationName;
import io.crate.role.Roles;
import io.crate.role.Securable;

@Singleton
public class InformationSchemaTableDefinitions {

    private final Map<RelationName, StaticTableDefinition<?>> tableDefinitions;

    @Inject
    public InformationSchemaTableDefinitions(Roles roles,
                                             InformationSchemaIterables informationSchemaIterables) {
        tableDefinitions = Map.ofEntries(
            Map.entry(
                InformationSchemataTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::schemas,
                    (user, s) -> roles.hasAnyPrivilege(user, Securable.SCHEMA, s.name()),
                    InformationSchemataTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                InformationTablesTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::relations,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.ident().fqn())
                                // we also need to check for views which have privileges set
                                || roles.hasAnyPrivilege(user, Securable.VIEW, t.ident().fqn()),
                    InformationTablesTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                InformationViewsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::views,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.VIEW, t.ident().fqn()),
                    InformationViewsTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                InformationPartitionsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::partitions,
                    (user, p) -> roles.hasAnyPrivilege(user, Securable.TABLE, p.name().relationName().fqn()),
                    InformationPartitionsTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                InformationColumnsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::columns,
                    (user, c) -> (roles.hasAnyPrivilege(user, Securable.TABLE, c.relation().ident().fqn())
                                // we also need to check for views which have privileges set
                                || roles.hasAnyPrivilege(user, Securable.VIEW, c.relation().ident().fqn())
                                ) && !c.ref().isDropped(),
                    InformationColumnsTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                InformationTableConstraintsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::constraints,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.relationName().fqn()),
                    InformationTableConstraintsTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                InformationRoutinesTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::routines,
                    (user, r) -> roles.hasAnyPrivilege(user, Securable.SCHEMA, r.schema()),
                    InformationRoutinesTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                InformationSqlFeaturesTableInfo.IDENT,
                new StaticTableDefinition<>(
                    (_, _) -> completedFuture(informationSchemaIterables.features()),
                    InformationSqlFeaturesTableInfo.INSTANCE.expressions(),
                    false
                )
            ),
            Map.entry(
                InformationKeyColumnUsageTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::keyColumnUsage,
                    (user, k) -> roles.hasAnyPrivilege(user, Securable.TABLE, k.getFQN()),
                    InformationKeyColumnUsageTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                InformationReferentialConstraintsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    (_, _) -> completedFuture(informationSchemaIterables.referentialConstraintsInfos()),
                    InformationReferentialConstraintsTableInfo.INSTANCE.expressions(),
                    false
                )
            ),
            Map.entry(
                InformationCharacterSetsTable.IDENT,
                new StaticTableDefinition<>(
                    (_, _) -> completedFuture(Arrays.asList(new Void[]{null})),
                    InformationCharacterSetsTable.INSTANCE.expressions(),
                    false
                )
            ),
            Map.entry(
                InformationEnabledRolesTableInfo.IDENT,
                new StaticTableDefinition<>(
                    (_, role) -> completedFuture(informationSchemaIterables.enabledRoles(role, roles)),
                    InformationEnabledRolesTableInfo.INSTANCE.expressions(),
                    false
                )
            ),
            Map.entry(
                InformationApplicableRolesTableInfo.IDENT,
                new StaticTableDefinition<>(
                    (_, role) -> completedFuture(informationSchemaIterables.applicableRoles(role, roles)),
                    InformationApplicableRolesTableInfo.INSTANCE.expressions(),
                    false
                )
            ),
            Map.entry(
                InformationRoleTableGrantsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    (_, role) -> completedFuture(informationSchemaIterables.roleTableGrants(role, roles)),
                    InformationRoleTableGrantsTableInfo.INSTANCE.expressions(),
                    false
                )
            ),
            Map.entry(
                InformationAdministrableRoleAuthorizationsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    (_, role) -> completedFuture(informationSchemaIterables.administrableRoleAuthorizations(role, roles)),
                    InformationAdministrableRoleAuthorizationsTableInfo.INSTANCE.expressions(),
                    false
                )
            ),
            Map.entry(
                ForeignServerTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::servers,
                    (user, t) -> user.isSuperUser() || t.owner().equals(user.name()),
                    ForeignServerTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                ForeignServerOptionsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::serverOptions,
                    (user, t) -> user.isSuperUser() || t.serverOwner().equals(user.name()),
                    ForeignServerOptionsTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                ForeignTableTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::foreignTables,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.name().fqn()),
                    ForeignTableTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                ForeignTableOptionsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::foreignTableOptions,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.relationName().fqn()),
                    ForeignTableOptionsTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                UserMappingsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::userMappings,
                    (user, _) -> roles.hasALPrivileges(user),
                    UserMappingsTableInfo.INSTANCE.expressions()
                )
            ),
            Map.entry(
                UserMappingOptionsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    (_, user) -> completedFuture(
                        () -> StreamSupport.stream(informationSchemaIterables.userMappingOptions().spliterator(), false)
                            .filter(_ -> roles.hasALPrivileges(user))
                            .map(userMappingOptions -> {
                                if ((user.name().equals(userMappingOptions.userName()) || user.isSuperUser()) == false) {
                                    return new UserMappingOptionsTableInfo.UserMappingOptions(
                                        userMappingOptions.userName(),
                                        userMappingOptions.serverName(),
                                        userMappingOptions.optionName(),
                                        null); // mask
                                }
                                return userMappingOptions;
                            })
                            .iterator()
                ),
                UserMappingOptionsTableInfo.INSTANCE.expressions(),
                true
            ))
        );
    }

    public StaticTableDefinition<?> get(RelationName relationName) {
        return tableDefinitions.get(relationName);
    }
}
