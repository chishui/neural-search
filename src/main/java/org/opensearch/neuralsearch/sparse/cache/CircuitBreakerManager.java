/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeValue;

public class CircuitBreakerManager {

    private static CircuitBreaker circuitBreaker;

    public synchronized static void setCircuitBreaker(@NonNull CircuitBreaker circuitBreaker) {
        CircuitBreakerManager.circuitBreaker = circuitBreaker;
    }

    /**
     * Updates memory usage for neural search operations
     *
     * @param bytes The number of bytes to add to the circuit breaker
     * @param label A label to identify the operation in case of circuit breaking
     * @throws CircuitBreakingException if the limit would be exceeded
     */
    public static void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, label);
    }

    /**
     * Updates memory usage for neural search operations without throwing exception
     *
     * @param bytes The number of bytes to add to the circuit breaker
     */
    public static void addWithoutBreaking(long bytes) {
        circuitBreaker.addWithoutBreaking(bytes);
    }

    /**
     * Decreases the tracked memory usage
     */
    public static void releaseBytes(long bytes) {
        circuitBreaker.addWithoutBreaking(-bytes);
    }

    /**
     * Set the circuit breaker memory limit and overhead
     */
    public static void setLimitAndOverhead(ByteSizeValue limit, double overhead) {
        circuitBreaker.setLimitAndOverhead(limit.getBytes(), overhead);
    }
}
