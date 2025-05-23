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

package org.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class VersionTest {

    @Test
    public void test_compatible_current_version() {
        assertThat(Version.CURRENT.isCompatible(Version.V_4_8_4)).isFalse();
        assertThat(Version.CURRENT.isCompatible(Version.V_5_10_0)).isFalse();
        // 5.10.1 is compatible with current version even that it's minimum compatibility version is lower
        // -> real version precedes minimum compatibility version
        assertThat(Version.CURRENT.isCompatible(Version.V_5_10_1)).isTrue();
        // Current version is compatible with 5.10.1 as it's minimum compatibility version is compatible with 5.10.1
        assertThat(Version.V_5_10_1.isCompatible(Version.CURRENT)).isTrue();
    }

    @Test
    public void test_min_version_is_5_10_1() {
        assertThat(Version.CURRENT.minimumCompatibilityVersion()).isEqualTo(Version.V_5_10_1);
    }

    @Test
    public void test_version_minimum_compatibility() {
        assertThat(Version.fromString("2.0.0").minimumCompatibilityVersion()).isEqualTo(Version.fromString("1.1.0"));
        assertThat(Version.fromString("3.0.0").minimumCompatibilityVersion()).isEqualTo(Version.fromString("2.3.0"));

        assertThat(Version.V_4_0_0.minimumCompatibilityVersion()).isEqualTo(Version.fromString("3.0.0"));
        assertThat(Version.V_4_1_0.minimumCompatibilityVersion()).isEqualTo(Version.V_4_0_0);

        assertThat(Version.V_5_0_0.minimumCompatibilityVersion()).isEqualTo(Version.V_4_0_0);
        assertThat(Version.V_5_10_1.minimumCompatibilityVersion()).isEqualTo(Version.V_4_0_0);

        assertThat(Version.CURRENT.minimumCompatibilityVersion()).isEqualTo(Version.V_5_10_1);
    }

    @Test
    public void test_can_parse_next_patch_release_version() {
        int internalId = Version.CURRENT.internalId + 100;
        Version futureVersion = Version.fromId(internalId);
        assertThat(futureVersion.major).isEqualTo(Version.CURRENT.major);
        assertThat(futureVersion.minor).isEqualTo(Version.CURRENT.minor);
        assertThat(futureVersion.revision).isEqualTo((byte) (Version.CURRENT.revision + 1));
        assertThat(futureVersion.luceneVersion).isEqualTo(Version.CURRENT.luceneVersion);
    }

    @Test
    public void test_can_parse_next_minor_release_version() {
        int internalId = Version.CURRENT.internalId + 10000;
        Version futureVersion = Version.fromId(internalId);
        assertThat(futureVersion.major).isEqualTo(Version.CURRENT.major);
        assertThat(futureVersion.minor).isEqualTo((byte) (Version.CURRENT.minor + 1));
        assertThat(futureVersion.revision).isEqualTo(Version.CURRENT.revision);
        assertThat(futureVersion.luceneVersion).isEqualTo(Version.CURRENT.luceneVersion);
    }

    @Test
    public void test_can_parse_next_major_release_version() {
        int internalId = Version.CURRENT.internalId + 1000000;
        Version futureVersion = Version.fromId(internalId);
        assertThat(futureVersion.major).isEqualTo((byte) (Version.CURRENT.major + 1));
        assertThat(futureVersion.minor).isEqualTo(Version.CURRENT.minor);
        assertThat(futureVersion.revision).isEqualTo(Version.CURRENT.revision);
        assertThat(futureVersion.luceneVersion).isEqualTo(org.apache.lucene.util.Version.fromBits(
            Version.CURRENT.luceneVersion.major + 1, 0, 0)
        );
    }
}
