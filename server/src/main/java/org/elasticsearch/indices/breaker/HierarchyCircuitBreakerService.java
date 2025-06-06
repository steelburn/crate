/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.breaker;

import static java.util.Objects.requireNonNull;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * CircuitBreakerService that attempts to redistribute space between breakers
 * if tripped
 */
public class HierarchyCircuitBreakerService extends CircuitBreakerService {

    private static final Logger LOGGER = LogManager.getLogger(HierarchyCircuitBreakerService.class);

    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();
    private static final double PARENT_BREAKER_ESCAPE_HATCH_PERCENTAGE = 0.30;

    private final ConcurrentMap<String, ChildMemoryCircuitBreaker> breakers = new ConcurrentHashMap<>();

    public static final Setting<ByteSizeValue> TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.total.limit", "95%", Property.Dynamic, Property.NodeScope, Property.Exposed);

    public static final Setting<ByteSizeValue> REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.request.limit", "60%", Property.Dynamic, Property.NodeScope, Property.Exposed);

    public static final Setting<ByteSizeValue> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("network.breaker.inflight_requests.limit", "100%", Property.Dynamic, Property.NodeScope);

    public static final String QUERY = "query";

    public static final Setting<ByteSizeValue> QUERY_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "indices.breaker.query.limit", "60%", Property.Dynamic, Property.NodeScope, Property.Exposed);

    public static final String JOBS_LOG = "jobs_log";
    public static final Setting<ByteSizeValue> JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "stats.breaker.log.jobs.limit", "5%", Property.Dynamic, Property.NodeScope, Property.Exposed);

    public static final String OPERATIONS_LOG = "operations_log";
    public static final Setting<ByteSizeValue> OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "stats.breaker.log.operations.limit", "5%", Property.Dynamic, Property.NodeScope, Property.Exposed);

    public static final String BREAKING_EXCEPTION_MESSAGE =
        "[query] Data too large, data for [%s] would be larger than limit of [%d/%s]";

    private volatile BreakerSettings queryBreakerSettings;
    private volatile BreakerSettings logJobsBreakerSettings;
    private volatile BreakerSettings logOperationsBreakerSettings;

    private volatile BreakerSettings parentSettings;
    private volatile BreakerSettings inFlightRequestsSettings;
    private volatile BreakerSettings requestSettings;

    // Tripped count for when redistribution was attempted but wasn't successful
    private final AtomicLong parentTripCount = new AtomicLong(0);

    public HierarchyCircuitBreakerService(Settings settings, ClusterSettings clusterSettings) {
        this.inFlightRequestsSettings = new BreakerSettings(
            CircuitBreaker.IN_FLIGHT_REQUESTS,
            IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes()
        );

        this.requestSettings = new BreakerSettings(
            CircuitBreaker.REQUEST,
            REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes()
        );

        this.parentSettings = new BreakerSettings(
            CircuitBreaker.PARENT,
            TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes()
        );

        queryBreakerSettings = new BreakerSettings(
            QUERY,
            QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes()
        );

        logJobsBreakerSettings = new BreakerSettings(
            JOBS_LOG,
            JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes()
        );

        logOperationsBreakerSettings = new BreakerSettings(
            OPERATIONS_LOG,
            OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes()
        );

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("parent circuit breaker with settings {}", this.parentSettings);
        }

        setBreaker(this.requestSettings);
        setBreaker(this.inFlightRequestsSettings);
        setBreaker(this.queryBreakerSettings);
        setBreaker(this.logJobsBreakerSettings);
        setBreaker(this.logOperationsBreakerSettings);

        clusterSettings.addSettingsUpdateConsumer(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, this::setTotalCircuitBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING, this::setInFlightRequestsBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, this::setRequestBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(
            QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
            (newLimit) -> setBreakerLimit(queryBreakerSettings, QUERY, s -> this.queryBreakerSettings = s, newLimit));
        clusterSettings.addSettingsUpdateConsumer(
            JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
            (newLimit) -> setBreakerLimit(logJobsBreakerSettings, JOBS_LOG, s -> this.logJobsBreakerSettings = s, newLimit));
        clusterSettings.addSettingsUpdateConsumer(
            OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
            (newLimit) ->
                setBreakerLimit(logOperationsBreakerSettings, OPERATIONS_LOG, s -> this.logOperationsBreakerSettings = s, newLimit));
    }

    public static String breakingExceptionMessage(String label, long limit) {
        return String.format(
            Locale.ENGLISH,
            BREAKING_EXCEPTION_MESSAGE,
            label,
            limit,
            new ByteSizeValue(limit)
        );
    }

    private void setRequestBreakerLimit(ByteSizeValue newRequestMax) {
        BreakerSettings newRequestSettings = new BreakerSettings(
            CircuitBreaker.REQUEST,
            newRequestMax.getBytes()
        );
        setBreaker(newRequestSettings);
        HierarchyCircuitBreakerService.this.requestSettings = newRequestSettings;
        LOGGER.info("Updated breaker settings request: {}", newRequestSettings);
    }

    private void setInFlightRequestsBreakerLimit(ByteSizeValue newInFlightRequestsMax) {
        BreakerSettings newInFlightRequestsSettings = new BreakerSettings(
            CircuitBreaker.IN_FLIGHT_REQUESTS,
            newInFlightRequestsMax.getBytes()
        );
        setBreaker(newInFlightRequestsSettings);
        HierarchyCircuitBreakerService.this.inFlightRequestsSettings = newInFlightRequestsSettings;
        LOGGER.info("Updated breaker settings for in-flight requests: {}", newInFlightRequestsSettings);
    }

    private void setTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.getBytes());
        this.parentSettings = newParentSettings;
    }

    private void setBreakerLimit(BreakerSettings oldSettings,
                                 String breakerName,
                                 Consumer<BreakerSettings> settingsConsumer,
                                 ByteSizeValue newLimit) {
        long newLimitBytes = newLimit == null ? oldSettings.bytesLimit() : newLimit.getBytes();
        BreakerSettings newSettings = new BreakerSettings(breakerName, newLimitBytes);
        setBreaker(newSettings);
        settingsConsumer.accept(newSettings);
        LOGGER.info("[{}] Updated breaker settings: {}", breakerName, newSettings);
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return this.breakers.get(name);
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        if (CircuitBreaker.PARENT.equals(name)) {
            return new CircuitBreakerStats(
                CircuitBreaker.PARENT,
                parentSettings.bytesLimit(),
                parentUsed(0L),
                parentTripCount.get(),
                1.0d
            );
        }
        CircuitBreaker breaker = requireNonNull(this.breakers.get(name), "Unknown circuit breaker: " + name);
        return new CircuitBreakerStats(
            breaker.getName(),
            breaker.getLimit(),
            breaker.getUsed(),
            breaker.getTrippedCount(),
            1.0d
        );
    }

    private long parentUsed(long newBytesReserved) {
        return currentMemoryUsage() + newBytesReserved;
    }

    //package private to allow overriding it in tests
    long currentMemoryUsage() {
        try {
            return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
        } catch (IllegalArgumentException ex) {
            // This exception can happen (rarely) due to a race condition in the JVM when determining usage of memory pools. We do not want
            // to fail requests because of this and thus return zero memory usage in this case. While we could also return the most
            // recently determined memory usage, we would overestimate memory usage immediately after a garbage collection event.
            assert ex.getMessage().matches("committed = \\d+ should be < max = \\d+");
            LOGGER.info("Cannot determine current memory usage due to JDK-8207200.", ex);
            return 0;
        }
    }

    /**
     * Checks whether the parent breaker has been tripped
     */
    public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
        long totalUsed = parentUsed(newBytesReserved);
        long parentLimit = this.parentSettings.bytesLimit();
        if (totalUsed > parentLimit) {
            long breakersTotalUsed = breakers.values().stream()
                .mapToLong(CircuitBreaker::getUsed)
                .sum();
            // if the individual breakers hardly use any memory we assume that there is a lot of heap usage by objects which can be GCd.
            // We want to allow the query so that it triggers GCs
            if ((breakersTotalUsed + newBytesReserved) < (parentLimit * PARENT_BREAKER_ESCAPE_HATCH_PERCENTAGE)) {
                return;
            }
            this.parentTripCount.incrementAndGet();
            throw new CircuitBreakingException(newBytesReserved, totalUsed, parentLimit, "parent: " + label);
        }
    }

    private void setBreaker(BreakerSettings breakerSettings) {
        breakers.compute(
            breakerSettings.name(),
            (_, oldBreaker) -> new ChildMemoryCircuitBreaker(breakerSettings, oldBreaker, this));
    }
}
